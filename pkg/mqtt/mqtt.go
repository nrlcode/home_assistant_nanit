package mqtt

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// ErrBabyNotAuthorized indicates a command was received for an unauthorized baby.
var ErrBabyNotAuthorized = errors.New("baby UID not authorized")

type SendLightCommandHandler func(nightLightState bool)
type SendStandbyCommandHandler func(standbyState bool)

// eventActiveWindow - how long a motion/sound "_active" binary_sensor stays on
// after an event (Nanit only gives us the event timestamp, not a duration).
const eventActiveWindow = 45 * time.Second

// timer controls a scheduled active-off callback; Stop prevents a stale
// generation from publishing after teardown or supersession.
type timer interface {
	Stop() bool
}

type activeTimer struct {
	generation int
	t          timer
}

// Connection - MQTT context
type Connection struct {
	Opts         Opts
	StateManager *baby.StateManager
	client       MQTT.Client

	babies   map[string]string // uid -> name, for HA discovery
	babiesMu sync.RWMutex
	OnEvent  func(babyUID, key string, epoch int32)

	sendLightCommandHandler   SendLightCommandHandler
	sendStandbyCommandHandler SendStandbyCommandHandler

	mu        sync.Mutex
	timers    map[string]*activeTimer // "uid/key" -> timer
	genSeq    map[string]int          // next generation per key, survives invalidate
	closed    bool
	shutdown  bool // set by runMqtt teardown; rejects stale post-shutdown connects
	afterFunc func(d time.Duration, f func()) timer
	connectWg sync.WaitGroup // tracks in-flight successful-connect callbacks
}

// NewConnection - constructor
func NewConnection(opts Opts) *Connection {
	return &Connection{
		Opts:      opts,
		babies:    map[string]string{},
		timers:    map[string]*activeTimer{},
		genSeq:    map[string]int{},
		afterFunc: func(d time.Duration, f func()) timer { return time.AfterFunc(d, f) },
	}
}

// EnsureClient creates the underlying Paho client synchronously if needed.
// Call before GetClient so notification wiring never captures a nil client,
// without polling sleeps. Safe for concurrent use; Run reuses it.
func (conn *Connection) EnsureClient() MQTT.Client {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.client == nil {
		conn.client = MQTT.NewClient(conn.buildClientOptionsLocked())
	}
	return conn.client
}

// SetBabies registers uid -> name so HA discovery can name the device.
// Call before Run(). Kept for WColan compatibility; delegates to RegisterBabies.
func (conn *Connection) SetBabies(babies []baby.Baby) {
	conn.RegisterBabies(babies)
}

// RegisterBaby registers a baby for MQTT discovery.
func (conn *Connection) RegisterBaby(babyUID, babyName string) {
	conn.babiesMu.Lock()
	if conn.babies == nil {
		conn.babies = map[string]string{}
	}
	conn.babies[babyUID] = babyName
	conn.babiesMu.Unlock()

	// If already connected, publish immediately (best effort; failures
	// are retried on the next successful connection).
	if client := conn.GetClient(); client != nil && client.IsConnected() {
		conn.publishDiscovery(babyUID, babyName)
	}
}

// RegisterBabies registers multiple babies for MQTT discovery.
func (conn *Connection) RegisterBabies(babies []baby.Baby) {
	for _, b := range babies {
		conn.babiesMu.Lock()
		if conn.babies == nil {
			conn.babies = map[string]string{}
		}
		conn.babies[b.UID] = b.Name
		conn.babiesMu.Unlock()
	}
}

// IsAuthorizedBaby checks if a baby UID is registered and authorized for commands.
func (conn *Connection) IsAuthorizedBaby(babyUID string) bool {
	if babyUID == "" {
		return false
	}
	conn.babiesMu.RLock()
	_, exists := conn.babies[babyUID]
	conn.babiesMu.RUnlock()
	return exists
}

// ValidateBabyCommandAuth validates that a baby UID is both valid and authorized.
func (conn *Connection) ValidateBabyCommandAuth(babyUID string) error {
	if babyUID == "" {
		return errors.New("baby UID cannot be empty")
	}
	if err := baby.ValidateBabyUID(babyUID); err != nil {
		return fmt.Errorf("invalid baby UID format: %w", err)
	}
	if !conn.IsAuthorizedBaby(babyUID) {
		return ErrBabyNotAuthorized
	}
	return nil
}

// GetTopicPrefix returns the MQTT topic prefix.
func (conn *Connection) GetTopicPrefix() string {
	return conn.Opts.TopicPrefix
}

// GetClient returns the underlying MQTT client.
func (conn *Connection) GetClient() MQTT.Client {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return conn.client
}

// buildClientOptionsLocked constructs Paho options with the actual
// loss/reconnect lifecycle. Caller must hold conn.mu.
func (conn *Connection) buildClientOptionsLocked() *MQTT.ClientOptions {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(conn.Opts.BrokerURL)
	opts.SetClientID(conn.clientID())
	opts.SetUsername(conn.Opts.Username)
	opts.SetPassword(conn.Opts.Password)
	opts.SetCleanSession(false)
	// Last will so HA marks the entities unavailable if the bridge dies
	opts.SetWill(conn.statusTopic(), "offline", 1, true)
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(func(client MQTT.Client) {
		conn.handleSuccessfulConnect(client)
	})
	opts.SetConnectionLostHandler(func(_ MQTT.Client, _ error) {
		conn.handleConnectionLost()
	})
	return opts
}

// handleSuccessfulConnect is the single successful-connect lifecycle owner
// (Paho OnConnect). It republishes availability, discovery configs, command
// subscriptions and active-false init on every successful (re)connection. It
// restores retained-data loss and re-arms availability; motion/sound windows
// never survive a reconnect. Reset and baseline publishes are serialized with
// events/teardown under mu; stale shutdown callbacks are rejected; genSeq is
// preserved so generations stay monotonic.
func (conn *Connection) handleSuccessfulConnect(client MQTT.Client) {
	conn.mu.Lock()
	if conn.shutdown {
		conn.mu.Unlock()
		return
	}
	conn.connectWg.Add(1)
	conn.mu.Unlock()
	defer conn.connectWg.Done()

	conn.babiesMu.RLock()
	babies := make(map[string]string, len(conn.babies))
	for uid, name := range conn.babies {
		babies[uid] = name
	}
	conn.babiesMu.RUnlock()

	conn.mu.Lock()
	if conn.shutdown {
		conn.mu.Unlock()
		return
	}
	conn.closed = false
	for k, entry := range conn.timers {
		if entry != nil && entry.t != nil {
			entry.t.Stop()
		}
		delete(conn.timers, k)
	}
	if client != nil {
		client.Publish(conn.statusTopic(), 1, true, "online")
		for uid := range babies {
			for _, key := range []string{"motion_active", "sound_active"} {
				topic := fmt.Sprintf("%v/babies/%v/%v", conn.Opts.TopicPrefix, uid, key)
				client.Publish(topic, 0, false, "false")
			}
		}
	}
	conn.mu.Unlock()

	for uid, name := range babies {
		conn.mu.Lock()
		shutdown := conn.shutdown
		conn.mu.Unlock()
		if shutdown {
			return
		}
		conn.publishDiscovery(uid, name)
	}
	conn.mu.Lock()
	shutdown := conn.shutdown
	conn.mu.Unlock()
	if shutdown {
		return
	}
	conn.subscribeToLightCommand()
	conn.mu.Lock()
	shutdown = conn.shutdown
	conn.mu.Unlock()
	if shutdown {
		return
	}
	conn.subscribeToStandbyCommand()
}

// handleConnectionLost invalidates pending active-off timers so stale
// callbacks cannot publish after the connection drops.
func (conn *Connection) handleConnectionLost() {
	conn.invalidateTimers()
}

func (conn *Connection) statusTopic() string {
	return fmt.Sprintf("%v/status", conn.Opts.TopicPrefix)
}

func (conn *Connection) clientID() string {
	if conn.Opts.ClientID != "" {
		return conn.Opts.ClientID
	}
	return "nanit"
}

// parseCommand strips the whole configured prefix and extracts baby UID + command.
// Expected remainder: babies/<uid>/<device>/switch
func (conn *Connection) parseCommand(fullTopic string) (babyUID, command string, ok bool) {
	prefix := strings.Trim(conn.Opts.TopicPrefix, "/")
	rest := strings.Trim(fullTopic, "/")
	if prefix != "" {
		prefixParts := strings.Split(prefix, "/")
		topicParts := strings.Split(rest, "/")
		if len(topicParts) < len(prefixParts) {
			return "", "", false
		}
		for i, p := range prefixParts {
			if topicParts[i] != p {
				return "", "", false
			}
		}
		rest = strings.Join(topicParts[len(prefixParts):], "/")
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 4 {
		return "", "", false
	}
	if parts[0] != "babies" || parts[3] != "switch" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// Run - runs the mqtt connection handler
func (conn *Connection) Run(manager *baby.StateManager, ctx utils.GracefulContext) {
	conn.StateManager = manager

	// Synchronous ownership: the client object exists before any async
	// attempt runs, so App wiring that snapshots GetClient never sees nil.
	conn.EnsureClient()

	utils.RunWithPerseverance(func(attempt utils.AttemptContext) {
		runMqtt(conn, attempt)
	}, ctx, utils.PerseverenceOpts{
		RunnerID:       "mqtt",
		ResetThreshold: 2 * time.Second,
		Cooldown: []time.Duration{
			2 * time.Second,
			10 * time.Second,
			1 * time.Minute,
		},
	})
}

func (conn *Connection) RegisterLightHandler(sendLightCommandHandler SendLightCommandHandler) {
	conn.sendLightCommandHandler = sendLightCommandHandler
}

func (conn *Connection) subscribeToLightCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/night_light/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	lightMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		babyUID, command, ok := conn.parseCommand(msg.Topic())
		if !ok {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}
		if err := conn.ValidateBabyCommandAuth(babyUID); err != nil {
			log.Error().Err(err).Str("baby_uid", babyUID).Msg("Unauthorized MQTT command rejected")
			return
		}
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received light command")
			if conn.sendLightCommandHandler != nil {
				conn.sendLightCommandHandler(enabled)
			}
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.GetClient().Subscribe(commandTopic, 0, lightMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

func (conn *Connection) RegisterStandyHandler(sendStandbyCommandHandler SendStandbyCommandHandler) {
	conn.sendStandbyCommandHandler = sendStandbyCommandHandler
}

func (conn *Connection) subscribeToStandbyCommand() {
	commandTopic := fmt.Sprintf("%v/babies/+/standby/switch", conn.Opts.TopicPrefix)
	log.Debug().
		Str("topic", commandTopic).
		Msg("Subscribing to command topic")

	standbyMessageHandler := func(mqttConn MQTT.Client, msg MQTT.Message) {
		babyUID, command, ok := conn.parseCommand(msg.Topic())
		if !ok {
			log.Error().Str("topic", msg.Topic()).Msg("Invalid command topic format")
			return
		}
		if err := conn.ValidateBabyCommandAuth(babyUID); err != nil {
			log.Error().Err(err).Str("baby_uid", babyUID).Msg("Unauthorized MQTT command rejected")
			return
		}
		switch command {
		case "switch":
			enabled := string(msg.Payload()) == "true"
			log.Debug().
				Str("baby", babyUID).
				Bool("enabled", enabled).
				Str("payload", string(msg.Payload())).
				Msg("Received standby command")
			if conn.sendStandbyCommandHandler != nil {
				conn.sendStandbyCommandHandler(enabled)
			}
		default:
			log.Warn().Str("command", command).Msg("Unknown command received")
		}
	}

	if token := conn.GetClient().Subscribe(commandTopic, 0, standbyMessageHandler); token.Wait() && token.Error() != nil {
		log.Error().Err(token.Error()).Str("topic", commandTopic).Msg("Failed to subscribe to command topic")
	}
}

func runMqtt(conn *Connection, attempt utils.AttemptContext) {
	conn.mu.Lock()
	conn.shutdown = false
	conn.mu.Unlock()
	client := conn.GetClient()
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Error().Str("broker_url", conn.Opts.BrokerURL).Err(token.Error()).Msg("Unable to connect to MQTT broker")
		attempt.Fail(token.Error())
		return
	}

	log.Info().Str("broker_url", conn.Opts.BrokerURL).Msg("Successfully connected to MQTT broker")

	// Successful-connect lifecycle is owned solely by the Paho OnConnect
	// callback (handleSuccessfulConnect), which Paho invokes on the initial
	// connect as well as auto-reconnects.

	unsubscribe := conn.StateManager.Subscribe(func(babyUID string, state baby.State) {
		publish := func(key string, value interface{}, retained bool) {
			conn.mu.Lock()
			shutdown := conn.shutdown
			conn.mu.Unlock()
			if shutdown {
				return
			}
			topic := fmt.Sprintf("%v/babies/%v/%v", conn.Opts.TopicPrefix, babyUID, key)
			log.Trace().Str("topic", topic).Interface("value", value).Bool("retained", retained).Msg("MQTT publish")

			token := client.Publish(topic, 0, retained, fmt.Sprintf("%v", value))
			if token.Wait(); token.Error() != nil {
				log.Error().Err(token.Error()).Msgf("Unable to publish %v update", key)
			}
		}

		for key, value := range state.AsMap(false) {
			publish(key, value, false)
		}

		// Derive HA-friendly motion/sound topics from the raw event timestamps.
		if state.MotionTimestamp != nil {
			conn.publishEvent(babyUID, "motion", *state.MotionTimestamp)
		}
		if state.SoundTimestamp != nil {
			conn.publishEvent(babyUID, "sound", *state.SoundTimestamp)
		}

		if state.StreamState != nil && *state.StreamState != baby.StreamState_Unknown {
			publish("is_stream_alive", *state.StreamState == baby.StreamState_Alive, true)
		}
	})

	// Wait until interrupt signal is received
	<-attempt.Done()

	log.Debug().Msg("Closing MQTT connection on interrupt")
	conn.mu.Lock()
	conn.shutdown = true
	conn.mu.Unlock()
	conn.invalidateTimers()
	unsubscribe()
	conn.connectWg.Wait()
	client.Publish(conn.statusTopic(), 1, true, "offline")
	client.Disconnect(250)
}

// PublishMotionSoundEvent publishes a motion/sound event through the single
// WColan-compatible writer. Used by the notification manager callback.
func (conn *Connection) PublishMotionSoundEvent(babyUID, key string, epoch int32) {
	if key != "motion" && key != "sound" {
		return
	}
	conn.publishEvent(babyUID, key, epoch)
}

// publishEvent turns a raw motion/sound epoch into HA-friendly topics:
//
//	<key>        retained RFC3339 timestamp (for a device_class: timestamp sensor)
//	<key>_active "true" now, "false" after eventActiveWindow (device_class motion/sound)
//
// Publication is serialized with lifecycle invalidation under conn.mu so a
// stale callback, newer event, or teardown cannot interleave between the
// generation check and the publish. Generations are monotonic per key across
// reconnects (genSeq survives invalidate) so old/new aliases are impossible.
// The OnEvent hook runs after unlock; it must not reenter publishEvent.
func (conn *Connection) publishEvent(babyUID, key string, epoch int32) {
	conn.mu.Lock()
	if conn.closed {
		conn.mu.Unlock()
		return
	}
	if conn.timers == nil {
		conn.timers = map[string]*activeTimer{}
	}
	if conn.genSeq == nil {
		conn.genSeq = map[string]int{}
	}
	mapKey := babyUID + "/" + key
	conn.genSeq[mapKey]++
	gen := conn.genSeq[mapKey]
	if prev, hasPrev := conn.timers[mapKey]; hasPrev && prev.t != nil {
		prev.t.Stop()
	}
	conn.timers[mapKey] = &activeTimer{generation: gen}
	after := conn.afterFunc
	client := conn.client
	base := fmt.Sprintf("%v/babies/%v", conn.Opts.TopicPrefix, babyUID)
	ts := time.Unix(int64(epoch), 0).UTC().Format(time.RFC3339)
	activeTopic := fmt.Sprintf("%v/%v_active", base, key)
	client.Publish(fmt.Sprintf("%v/%v", base, key), 0, true, ts)
	client.Publish(activeTopic, 0, false, "true")
	t := after(eventActiveWindow, func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		cur, ok := conn.timers[mapKey]
		if !ok || cur.generation != gen || conn.closed {
			return
		}
		client.Publish(activeTopic, 0, false, "false")
	})
	if cur, ok := conn.timers[mapKey]; ok && cur.generation == gen {
		cur.t = t
	} else if t != nil {
		t.Stop()
	}
	onEvent := conn.OnEvent
	conn.mu.Unlock()

	if onEvent != nil {
		// Non-blocking hook for tests/observers; never fails the publish path.
		func() {
			defer func() { _ = recover() }()
			onEvent(babyUID, key, epoch)
		}()
	}
}

// invalidateTimers stops all pending active-off timers so callbacks cannot
// publish after teardown. Called on disconnect/shutdown. Generation sequence
// is preserved so reconnect generations never alias pre-drop generations.
func (conn *Connection) invalidateTimers() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.closed = true
	for k, entry := range conn.timers {
		if entry != nil && entry.t != nil {
			entry.t.Stop()
		}
		delete(conn.timers, k)
	}
}
