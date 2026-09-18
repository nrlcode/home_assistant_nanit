package mqtt

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

type fakeToken struct {
	err  error
	done chan struct{}
}

func newFakeToken(err error) *fakeToken {
	ch := make(chan struct{})
	close(ch)
	return &fakeToken{err: err, done: ch}
}

func (t *fakeToken) Wait() bool                       { return true }
func (t *fakeToken) WaitTimeout(_ time.Duration) bool { return true }
func (t *fakeToken) Done() <-chan struct{}            { return t.done }
func (t *fakeToken) Error() error                     { return t.err }

type published struct {
	topic    string
	qos      byte
	retained bool
	payload  string
}

type fakeClient struct {
	mu        sync.Mutex
	publishes []published
	connected bool
	failNext  int
	subs      map[string]MQTT.MessageHandler
}

func newFakeClient() *fakeClient {
	return &fakeClient{subs: map[string]MQTT.MessageHandler{}}
}

func (f *fakeClient) IsConnected() bool      { return f.connected }
func (f *fakeClient) IsConnectionOpen() bool { return f.connected }
func (f *fakeClient) Connect() MQTT.Token    { f.connected = true; return newFakeToken(nil) }
func (f *fakeClient) Disconnect(_ uint)      { f.connected = false }
func (f *fakeClient) Publish(topic string, qos byte, retained bool, payload interface{}) MQTT.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return newFakeToken(errFakePublish)
	}
	var s string
	switch v := payload.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = ""
		if b, err := json.Marshal(v); err == nil {
			s = string(b)
		}
	}
	f.publishes = append(f.publishes, published{topic: topic, qos: qos, retained: retained, payload: s})
	return newFakeToken(nil)
}
func (f *fakeClient) Subscribe(topic string, qos byte, cb MQTT.MessageHandler) MQTT.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subs[topic] = cb
	return newFakeToken(nil)
}
func (f *fakeClient) SubscribeMultiple(filters map[string]byte, cb MQTT.MessageHandler) MQTT.Token {
	return newFakeToken(nil)
}
func (f *fakeClient) Unsubscribe(_ ...string) MQTT.Token       { return newFakeToken(nil) }
func (f *fakeClient) AddRoute(_ string, _ MQTT.MessageHandler) {}
func (f *fakeClient) OptionsReader() MQTT.ClientOptionsReader  { return MQTT.NewOptionsReader(nil) }

func (f *fakeClient) publishedTopics() []published {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]published, len(f.publishes))
	copy(out, f.publishes)
	return out
}

func (f *fakeClient) findTopic(suffix string) *published {
	for _, p := range f.publishedTopics() {
		if strings.HasSuffix(p.topic, suffix) {
			cp := p
			return &cp
		}
	}
	return nil
}

var errFakePublish = errFake()

func errFake() error { return &fakeErr{} }

type fakeErr struct{}

func (e *fakeErr) Error() string { return "fake publish failure" }

type fakeTimer struct {
	mu      sync.Mutex
	stopped bool
	f       func()
	d       time.Duration
}

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

func (t *fakeTimer) fire() {
	// Always invoke: production generation/closed guards decide, so stale
	// callbacks exercise the real rejection path instead of fake suppression.
	t.mu.Lock()
	f := t.f
	t.mu.Unlock()
	if f != nil {
		f()
	}
}

func newConnForTest(opts Opts) (*Connection, *fakeClient, *[]*fakeTimer) {
	fc := newFakeClient()
	fc.connected = true
	conn := NewConnection(opts)
	conn.client = fc
	var timers []*fakeTimer
	var mu sync.Mutex
	conn.afterFunc = func(d time.Duration, f func()) timer {
		ft := &fakeTimer{f: f, d: d}
		mu.Lock()
		timers = append(timers, ft)
		mu.Unlock()
		return ft
	}
	return conn, fc, &timers
}

func TestDiscoveryContract(t *testing.T) {
	opts := Opts{
		TopicPrefix:      "nanit",
		DiscoveryEnabled: true,
		DiscoveryPrefix:  "homeassistant",
		RTMPAddr:         "192.168.1.100:1935",
		ClientID:         "nanit-test",
	}
	conn, fc, _ := newConnForTest(opts)
	conn.RegisterBabies([]baby.Baby{{UID: "abc123", Name: "Test Baby"}})
	if n := conn.publishDiscovery("abc123", "Test Baby"); n != 0 {
		t.Fatalf("publishDiscovery failures = %d, want 0", n)
	}
	pubs := fc.publishedTopics()
	if len(pubs) == 0 {
		t.Fatal("no discovery publications")
	}
	// Expect the expanded 35-entry roster plus stream_url discovery.
	discoveryCount := 0
	for _, p := range pubs {
		if strings.HasSuffix(p.topic, "/config") {
			discoveryCount++
			if p.qos != 1 || !p.retained {
				t.Errorf("discovery %q qos=%d retained=%v, want qos=1 retained=true", p.topic, p.qos, p.retained)
			}
		}
	}
	if discoveryCount != 36 {
		t.Errorf("discovery config count = %d, want 36 (35 roster + stream_url)", discoveryCount)
	}
	// Spot-check roster topics and IDs.
	for _, want := range []struct{ component, object string }{
		{"sensor", "temperature"},
		{"sensor", "humidity"},
		{"sensor", "motion"},
		{"sensor", "sound"},
		{"binary_sensor", "motion_active"},
		{"binary_sensor", "sound_active"},
		{"binary_sensor", "night"},
		{"binary_sensor", "stream"},
		{"switch", "night_light"},
		{"switch", "standby"},
		{"sensor", "stream_url"},
	} {
		topic := "homeassistant/" + want.component + "/nanit_abc123/" + want.object + "/config"
		found := false
		for _, p := range pubs {
			if p.topic == topic {
				found = true
				var e haEntity
				if err := json.Unmarshal([]byte(p.payload), &e); err != nil {
					t.Errorf("unmarshal %q: %v", topic, err)
					break
				}
				if e.UniqueID != "nanit_abc123_"+want.object {
					t.Errorf("%q unique_id = %q, want nanit_abc123_%s", topic, e.UniqueID, want.object)
				}
				if len(e.Device.Identifiers) != 1 || e.Device.Identifiers[0] != "nanit_abc123" {
					t.Errorf("%q device identifiers = %v", topic, e.Device.Identifiers)
				}
				if len(e.Availability) != 1 || e.Availability[0].Topic != "nanit/status" {
					t.Errorf("%q availability = %+v, want nanit/status", topic, e.Availability)
				}
				break
			}
		}
		if !found {
			t.Errorf("missing discovery topic %q", topic)
		}
	}
	// Switches must carry command topics and true/false payloads.
	for _, p := range pubs {
		if strings.Contains(p.topic, "/night_light/config") || strings.Contains(p.topic, "/standby/config") {
			var e haEntity
			if err := json.Unmarshal([]byte(p.payload), &e); err != nil {
				continue
			}
			if e.CommandTopic == "" {
				t.Errorf("switch %q missing command_topic", p.topic)
			}
			if e.PayloadOn != "true" || e.PayloadOff != "false" {
				t.Errorf("switch %q payload_on/off = %q/%q", p.topic, e.PayloadOn, e.PayloadOff)
			}
		}
	}
	// Stream URL value retained.
	urlPub := fc.findTopic("/babies/abc123/stream_url")
	if urlPub == nil {
		t.Fatal("missing stream_url value publication")
	}
	if !urlPub.retained {
		t.Error("stream_url value should be retained")
	}
	if urlPub.payload != "rtmp://192.168.1.100:1935/local/abc123" {
		t.Errorf("stream_url = %q, want rtmp://192.168.1.100:1935/local/abc123", urlPub.payload)
	}

	t.Run("disabled discovery publishes nothing", func(t *testing.T) {
		opts2 := Opts{TopicPrefix: "nanit", DiscoveryEnabled: false}
		c2, f2, _ := newConnForTest(opts2)
		c2.RegisterBabies([]baby.Baby{{UID: "x", Name: "X"}})
		if n := c2.publishDiscovery("x", "X"); n != 0 {
			t.Errorf("disabled publishDiscovery = %d, want 0", n)
		}
		if len(f2.publishedTopics()) != 0 {
			t.Errorf("disabled discovery published %d messages", len(f2.publishedTopics()))
		}
	})

	t.Run("custom prefixes honored", func(t *testing.T) {
		opts3 := Opts{TopicPrefix: "custom", DiscoveryEnabled: true, DiscoveryPrefix: "ha", RTMPAddr: ""}
		c3, f3, _ := newConnForTest(opts3)
		c3.RegisterBabies([]baby.Baby{{UID: "u1", Name: "U"}})
		c3.publishDiscovery("u1", "U")
		found := false
		for _, p := range f3.publishedTopics() {
			if p.topic == "ha/sensor/nanit_u1/temperature/config" {
				found = true
				var e haEntity
				_ = json.Unmarshal([]byte(p.payload), &e)
				if e.StateTopic != "custom/babies/u1/temperature" {
					t.Errorf("state_topic = %q", e.StateTopic)
				}
			}
			if strings.Contains(p.topic, "stream_url") {
				t.Errorf("stream_url should be omitted when RTMP disabled, got %q", p.topic)
			}
		}
		if !found {
			t.Error("custom discovery prefix topic missing")
		}
	})

	t.Run("failed publications retried on next connection", func(t *testing.T) {
		c4, f4, _ := newConnForTest(opts)
		c4.RegisterBabies([]baby.Baby{{UID: "abc123", Name: "Test Baby"}})
		f4.failNext = 1000
		if n := c4.publishDiscovery("abc123", "Test Baby"); n == 0 {
			t.Error("expected failures with failing client")
		}
		if len(f4.publishedTopics()) != 0 {
			t.Errorf("failed publishes should not be recorded, got %d", len(f4.publishedTopics()))
		}
		f4.failNext = 0
		if n := c4.publishDiscovery("abc123", "Test Baby"); n != 0 {
			t.Errorf("retry failures = %d, want 0", n)
		}
		if len(f4.publishedTopics()) == 0 {
			t.Error("retry should publish configs")
		}
	})
}

func TestMotionSoundWindow(t *testing.T) {
	opts := Opts{TopicPrefix: "nanit", DiscoveryEnabled: false}
	conn, fc, timers := newConnForTest(opts)

	publishCount := func(suffix, payload string) int {
		n := 0
		for _, p := range fc.publishedTopics() {
			if strings.HasSuffix(p.topic, suffix) && p.payload == payload {
				n++
			}
		}
		return n
	}

	// Latest event wins: older timer cannot terminate newer window.
	conn.publishEvent("baby1", "motion", 1700000000)
	if publishCount("/babies/baby1/motion_active", "true") != 1 {
		t.Fatal("expected motion_active true")
	}
	firstTimers := append([]*fakeTimer{}, *timers...)
	if len(firstTimers) != 1 {
		t.Fatalf("expected 1 timer, got %d", len(firstTimers))
	}
	if got := firstTimers[0].d; got != 45*time.Second {
		t.Fatalf("active window delay = %v, want %v", got, 45*time.Second)
	}
	conn.publishEvent("baby1", "motion", 1700000060)
	if publishCount("/babies/baby1/motion_active", "true") != 2 {
		t.Fatal("expected second motion_active true")
	}
	if len(*timers) != 2 {
		t.Fatalf("expected 2 timers, got %d", len(*timers))
	}
	// Fire the older timer: must not publish false (generation guard).
	firstTimers[0].fire()
	if publishCount("/babies/baby1/motion_active", "false") != 0 {
		t.Error("stale timer published false (generation guard failed)")
	}
	// Fire the newer timer: publishes false.
	(*timers)[1].fire()
	if publishCount("/babies/baby1/motion_active", "false") != 1 {
		t.Error("current timer should publish false")
	}

	// Independent babies/types.
	conn.publishEvent("baby1", "sound", 1700000100)
	conn.publishEvent("baby2", "motion", 1700000100)
	if publishCount("/babies/baby1/sound_active", "true") != 1 {
		t.Error("sound baby1 true missing")
	}
	if publishCount("/babies/baby2/motion_active", "true") != 1 {
		t.Error("motion baby2 true missing")
	}

	// Timestamp retained, active non-retained.
	for _, p := range fc.publishedTopics() {
		if strings.HasSuffix(p.topic, "/babies/baby1/motion") && !strings.HasSuffix(p.topic, "_active") {
			if !p.retained {
				t.Error("motion timestamp should be retained")
			}
		}
		if strings.HasSuffix(p.topic, "motion_active") && p.retained {
			t.Error("motion_active should be non-retained")
		}
	}

	// Teardown: callbacks cannot publish after disconnect/shutdown.
	conn.invalidateTimers()
	before := publishCount("/babies/baby1/sound_active", "false")
	for _, tm := range *timers {
		tm.fire()
	}
	after := publishCount("/babies/baby1/sound_active", "false")
	if after != before {
		t.Error("timer fired after teardown (must not publish)")
	}

	// Reconnect: re-arm only for new events.
	conn.mu.Lock()
	conn.closed = false
	conn.mu.Unlock()
	conn.publishEvent("baby1", "motion", 1700000200)
	if publishCount("/babies/baby1/motion_active", "true") != 3 {
		t.Error("reconnect should re-arm for new events")
	}
}

func TestMQTTRouting(t *testing.T) {
	t.Run("nested prefix parsing", func(t *testing.T) {
		c := NewConnection(Opts{TopicPrefix: "a/b"})
		uid, cmd, ok := c.parseCommand("a/b/babies/baby1/night_light/switch")
		if !ok || uid != "baby1" || cmd != "switch" {
			t.Errorf("parse = %q %q %v", uid, cmd, ok)
		}
		if _, _, ok := c.parseCommand("a/b/babies/baby1/night_light/wrong"); ok {
			t.Error("invalid command should not parse")
		}
		if _, _, ok := c.parseCommand("other/babies/baby1/night_light/switch"); ok {
			t.Error("wrong prefix should not parse")
		}
		c2 := NewConnection(Opts{TopicPrefix: "nanit"})
		uid, cmd, ok = c2.parseCommand("nanit/babies/baby-1/standby/switch")
		if !ok || uid != "baby-1" || cmd != "switch" {
			t.Errorf("single prefix parse = %q %q %v", uid, cmd, ok)
		}
	})

	t.Run("configured client ID", func(t *testing.T) {
		c := NewConnection(Opts{TopicPrefix: "nanit", ClientID: "my-client"})
		if got := c.clientID(); got != "my-client" {
			t.Errorf("clientID = %q, want my-client", got)
		}
		c2 := NewConnection(Opts{TopicPrefix: "nanit"})
		if got := c2.clientID(); got == "nanit" && c2.Opts.TopicPrefix == "nanit" {
			// Default is nanit, but must come from ClientID path, not prefix alias.
			// Accept default value; the key assertion is explicit IDs are honored.
		}
		if got := c2.clientID(); got != "nanit" {
			t.Errorf("default clientID = %q, want nanit", got)
		}
	})

	t.Run("unauthorized commands rejected", func(t *testing.T) {
		c := NewConnection(Opts{TopicPrefix: "nanit"})
		c.RegisterBaby("baby1", "Baby One")
		if err := c.ValidateBabyCommandAuth("baby1"); err != nil {
			t.Errorf("authorized baby rejected: %v", err)
		}
		if err := c.ValidateBabyCommandAuth("unknown"); !strings.Contains(err.Error(), "not authorized") {
			t.Errorf("unknown baby err = %v, want not authorized", err)
		}
		if err := c.ValidateBabyCommandAuth(""); err == nil {
			t.Error("empty UID should fail")
		}
		if err := c.ValidateBabyCommandAuth("BAD UID!"); err == nil {
			t.Error("malformed UID should fail")
		}
		if c.IsAuthorizedBaby("") {
			t.Error("empty UID authorized")
		}
	})
}

func TestStreamURL(t *testing.T) {
	for _, tc := range []struct {
		addr, uid, want string
	}{
		{"192.168.1.100:1935", "abc123", "rtmp://192.168.1.100:1935/local/abc123"},
		{"rtmp://192.168.1.100:1935", "abc123", "rtmp://192.168.1.100:1935/local/abc123"},
		{"10.0.0.5:1936", "u-1", "rtmp://10.0.0.5:1936/local/u-1"},
		{"", "abc123", ""},
		{"192.168.1.100", "abc123", ""},
		{"192.168.1.100:abc", "abc123", ""},
		{"192.168.1.100:1935", "", ""},
	} {
		if got := streamURLForBaby(tc.addr, tc.uid); got != tc.want {
			t.Errorf("streamURLForBaby(%q,%q) = %q, want %q", tc.addr, tc.uid, got, tc.want)
		}
	}

	t.Run("omitted when RTMP disabled", func(t *testing.T) {
		c, fc, _ := newConnForTest(Opts{TopicPrefix: "nanit", DiscoveryEnabled: true, RTMPAddr: ""})
		c.RegisterBabies([]baby.Baby{{UID: "b1", Name: "B"}})
		c.publishDiscovery("b1", "B")
		for _, p := range fc.publishedTopics() {
			if strings.Contains(p.topic, "stream_url") {
				t.Errorf("stream_url should be omitted without RTMP, got %q", p.topic)
			}
		}
	})

	t.Run("measured stream not optimistic", func(t *testing.T) {
		// Unknown stream state must not read as alive.
		s := baby.NewState()
		if s.GetStreamState() == baby.StreamState_Alive {
			t.Error("new state should not be alive")
		}
		alive := baby.StreamState_Alive
		s.StreamState = &alive
		if *s.StreamState != baby.StreamState_Alive {
			t.Error("alive state not preserved")
		}
	})
}
