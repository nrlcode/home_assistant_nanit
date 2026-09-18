package app

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"github.com/indiefan/home_assistant_nanit/pkg/notification"
	"github.com/indiefan/home_assistant_nanit/pkg/rtmpserver"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/stream"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// App - application container
type App struct {
	Opts             Opts
	SessionStore     *session.Store
	BabyStateManager *baby.StateManager
	RestClient       *client.NanitClient
	MQTTConnection   *mqtt.Connection

	// RTMP server reference for remote streaming support
	rtmpServer *rtmpserver.RTMPServer

	// Stream managers per baby (for failover support)
	streamManagersMu sync.RWMutex
	streamManagers   map[string]*stream.StreamManager

	// Notification manager for event polling
	notificationManager *notification.Manager
}

// NewApp - constructor
// Returns error if session store cannot be initialized
func NewApp(opts Opts) (*App, error) {
	sessionStore, err := session.InitSessionStore(opts.SessionFile)
	if err != nil {
		return nil, err
	}

	instance := &App{
		Opts:             opts,
		BabyStateManager: baby.NewStateManager(),
		SessionStore:     sessionStore,
		RestClient: &client.NanitClient{
			Email:        opts.NanitCredentials.Email,
			Password:     opts.NanitCredentials.Password,
			RefreshToken: opts.NanitCredentials.RefreshToken,
			SessionStore: sessionStore,
		},
		streamManagers: make(map[string]*stream.StreamManager),
	}

	if opts.MQTT != nil {
		instance.MQTTConnection = mqtt.NewConnection(*opts.MQTT)
	}

	return instance, nil
}

// Run - application main loop
func (app *App) Run(ctx utils.GracefulContext) {
	// Reauthorize if we don't have a token or we assume it is invalid
	if err := app.RestClient.MaybeAuthorize(false); err != nil {
		log.Error().Err(err).Msg("Failed to authorize with Nanit API")
		return
	}

	// Fetches babies info if they are not present in session
	if _, err := app.RestClient.EnsureBabies(); err != nil {
		log.Error().Err(err).Msg("Failed to fetch babies")
		return
	}

	// RTMP
	if app.Opts.RTMP != nil {
		// Use new secure server if any security options are configured
		if app.Opts.RTMP.AllowedIPs != "" || app.Opts.RTMP.AllowedPresets != "" {
			serverCfg := rtmpserver.ServerConfig{
				ListenAddr:     app.Opts.RTMP.ListenAddr,
				AllowedIPs:     app.Opts.RTMP.AllowedIPs,
				AllowedPresets: app.Opts.RTMP.AllowedPresets,
				LogDenied:      app.Opts.RTMP.LogDenied,
				LogAllowed:     app.Opts.RTMP.LogAllowed,
			}

			server, err := rtmpserver.NewServer(serverCfg, app.BabyStateManager)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to initialize RTMP server with security config")
				return
			}

			app.rtmpServer = server

			// Run server in background with graceful shutdown
			ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				go func() {
					<-childCtx.Done()
					server.Close()
				}()
				server.Run(app.Opts.RTMP.ListenAddr)
			})
		} else {
			// Empty rules deny all via NewServer's deny-all controller.
			log.Warn().Msg("Starting RTMP server without security config - all connections will be denied")
			server, err := rtmpserver.NewServer(rtmpserver.ServerConfig{
				ListenAddr: app.Opts.RTMP.ListenAddr,
			}, app.BabyStateManager)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to initialize RTMP server with deny-all config")
				return
			}
			app.rtmpServer = server
			ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				go func() {
					<-childCtx.Done()
					server.Close()
				}()
				server.Run(app.Opts.RTMP.ListenAddr)
			})
		}
	}

	// MQTT
	if app.MQTTConnection != nil {
		// Register babies for MQTT discovery before starting connection
		app.MQTTConnection.RegisterBabies(app.SessionStore.GetBabies())

		// Synchronous ownership: create the Paho client now so notification
		// wiring below never snapshots a nil client from the async Run.
		app.MQTTConnection.EnsureClient()
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			app.MQTTConnection.Run(app.BabyStateManager, childCtx)
		})

		// Single message polling loop, gated by the existing events switch.
		// The notification manager is the ONLY REST message poller here;
		// legacy pollMessages is not run in parallel.
		if app.Opts.EventPolling.Enabled {
			app.startNotificationPolling(ctx)
		}
	}

	// Start reading the data from the stream
	for _, babyInfo := range app.SessionStore.GetBabies() {
		_babyInfo := babyInfo
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			app.handleBaby(_babyInfo, childCtx)
		})
	}

	// Start serving content over HTTP
	if app.Opts.HTTPEnabled {
		go serve(app.SessionStore.GetBabies(), app.Opts.DataDirectories)
	}

	<-ctx.Done()
}

func (app *App) handleBaby(baby baby.Baby, ctx utils.GracefulContext) {
	if app.Opts.RTMP != nil || app.MQTTConnection != nil {
		// Websocket connection
		ws := client.NewWebsocketConnectionManager(baby.UID, baby.CameraUID, app.SessionStore.Session, app.RestClient, app.BabyStateManager)

		ws.WithReadyConnection(func(conn *client.WebsocketConnection, childCtx utils.GracefulContext) {
			app.runWebsocket(baby.UID, conn, childCtx)
		})

		// Legacy REST pollMessages is only a fallback when the notification
		// manager cannot run (events enabled but MQTT unavailable). When MQTT
		// is available the notification manager is the single message loop.
		if app.Opts.EventPolling.Enabled && app.MQTTConnection == nil {
			ctx.RunAsChild(func(childCtx utils.GracefulContext) {
				app.pollMessages(childCtx, baby.UID, app.BabyStateManager)
			})
		}

		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			ws.RunWithinContext(childCtx)
		})
	}

	<-ctx.Done()
}

func (app *App) pollMessages(ctx utils.GracefulContext, babyUID string, babyStateManager *baby.StateManager) {
	ticker := time.NewTicker(app.Opts.EventPolling.PollingInterval)
	defer ticker.Stop()

	// Poll immediately on start, then at each interval
	for {
		newMessages := app.RestClient.FetchNewMessages(babyUID, app.Opts.EventPolling.MessageTimeout)

		for _, msg := range newMessages {
			switch msg.Type {
			case message.SoundEventMessageType:
				go babyStateManager.NotifySoundSubscribers(babyUID, time.Time(msg.Time))
			case message.MotionEventMessageType:
				go babyStateManager.NotifyMotionSubscribers(babyUID, time.Time(msg.Time))
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Continue to next poll iteration
		}
	}
}

func (app *App) runWebsocket(babyUID string, conn *client.WebsocketConnection, childCtx utils.GracefulContext) {
	// Reading sensor data
	conn.RegisterMessageHandler(func(m *client.Message, conn *client.WebsocketConnection) {
		// Sensor request initiated by us on start (or some other client, we don't care)
		if *m.Type == client.Message_RESPONSE && m.Response != nil {
			if *m.Response.RequestType == client.RequestType_GET_SENSOR_DATA && len(m.Response.SensorData) > 0 {
				processSensorData(babyUID, m.Response.SensorData, app.BabyStateManager)
			} else if *m.Response.RequestType == client.RequestType_GET_CONTROL && m.Response.Control != nil {
				processLight(babyUID, m.Response.Control, app.BabyStateManager)
			} else if *m.Response.RequestType == client.RequestType_GET_SETTINGS && m.Response.Settings != nil {
				processStandby(babyUID, m.Response.Settings, app.BabyStateManager)
			}
		} else

		// Communication initiated from a cam
		// Note: it sends the updates periodically on its own + whenever some significant change occurs
		if *m.Type == client.Message_REQUEST && m.Request != nil {
			if *m.Request.Type == client.RequestType_PUT_SENSOR_DATA && len(m.Request.SensorData_) > 0 {
				processSensorData(babyUID, m.Request.SensorData_, app.BabyStateManager)
			} else if *m.Request.Type == client.RequestType_PUT_CONTROL && m.Request.Control != nil {
				processLight(babyUID, m.Request.Control, app.BabyStateManager)
			} else if *m.Request.Type == client.RequestType_PUT_SETTINGS && m.Request.Settings != nil {
				processStandby(babyUID, m.Request.Settings, app.BabyStateManager)
			}
		}
	})

	if app.Opts.MQTT != nil && app.MQTTConnection != nil {
		app.MQTTConnection.RegisterLightHandler(func(enabled bool) {
			sendLightCommand(enabled, conn)
		})
		app.MQTTConnection.RegisterStandyHandler(func(enabled bool) {
			sendStandbyCommand(enabled, conn)
		})
	}

	// Get the initial state of the light
	conn.SendRequest(client.RequestType_GET_CONTROL, &client.Request{GetControl_: &client.GetControl{
		NightLight: utils.ConstRefBool(true),
	}})

	// Ask for sensor data (initial request)
	conn.SendRequest(client.RequestType_GET_SENSOR_DATA, &client.Request{
		GetSensorData: &client.GetSensorData{
			All: utils.ConstRefBool(true),
		},
	})

	// Ask for status
	// conn.SendRequest(client.RequestType_GET_STATUS, &client.Request{
	// 	GetStatus_: &client.GetStatus{
	// 		All: utils.ConstRefBool(true),
	// 	},
	// })

	// Ask for logs
	// conn.SendRequest(client.RequestType_GET_LOGS, &client.Request{
	// 	GetLogs: &client.GetLogs{
	// 		Url: utils.ConstRefStr("http://192.168.3.234:8080/log"),
	// 	},
	// })

	var cleanup func()

	// Streaming (local with remote failover)
	if app.Opts.RTMP != nil {
		// Create StreamManager for this baby
		sm := app.createStreamManager(babyUID, conn)

		// Convert GracefulContext to standard context
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-childCtx.Done()
			cancel()
		}()

		// Start the stream manager
		startDone := make(chan struct{})
		go func() {
			defer close(startDone)
			if err := sm.Start(ctx); err != nil {
				// Error logged inside StreamManager
			}
		}()

		cleanup = func() {
			cancel()
			<-startDone
			sm.Stop()
			app.removeStreamManager(babyUID)
		}
	}

	<-childCtx.Done()
	if cleanup != nil {
		cleanup()
	}
}

// createStreamManager creates a StreamManager for the given baby
func (app *App) createStreamManager(babyUID string, conn *client.WebsocketConnection) *stream.StreamManager {
	// Use HybridStreamPool - local as primary, remote as hot standby
	// This provides true redundancy since local and remote use different network paths
	// When local stalls (camera issues), it seamlessly switches to remote using keyframe-aligned switching
	hybridPool := stream.NewHybridStreamPool(stream.HybridStreamPoolConfig{
		BabyUID:        babyUID,
		LocalURL:       app.getLocalStreamURL(babyUID), // e.g., rtmp://host/local/babyUID
		AuthToken:      app.SessionStore.GetAuthToken(),
		TokenProvider:  app.createTokenProvider(), // Provides fresh tokens when remote stream reconnects
		Conn:           conn,
		StateManager:   app.BabyStateManager,
		RTMPServer:     app.rtmpServer,
		StallThreshold: 3 * time.Second, // Switch to remote if no packets for 3s
		OnAuthFailed:   app.onAuthFailed,
	})

	// Create a no-op remote relay since HybridStreamPool manages its own remote connection
	// The StreamManager still expects a RemoteRelay but HybridStreamPool handles failover internally
	noopRelay := stream.NewNoopRelay()

	// Create the stream manager
	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       babyUID,
		StateManager:  app.BabyStateManager,
		LocalStreamer: hybridPool,
		RemoteRelay:   noopRelay,
		PreferRemote:  app.Opts.RTMP.PreferRemote,
	})

	// Store reference
	app.streamManagersMu.Lock()
	app.streamManagers[babyUID] = sm
	app.streamManagersMu.Unlock()

	return sm
}

// removeStreamManager removes the StreamManager for the given baby
func (app *App) removeStreamManager(babyUID string) {
	app.streamManagersMu.Lock()
	delete(app.streamManagers, babyUID)
	app.streamManagersMu.Unlock()
}

// getRemoteStreamURL builds the remote stream URL
// WARNING: The returned URL contains auth token - never log it directly
func (app *App) getRemoteStreamURL(babyUID string) string {
	return stream.BuildRemoteStreamURL(babyUID, app.SessionStore.GetAuthToken())
}

func (app *App) getLocalStreamURL(babyUID string) string {
	if app.Opts.RTMP != nil {
		tpl := "rtmp://{publicAddr}/local/{babyUid}"
		return strings.NewReplacer("{publicAddr}", app.Opts.RTMP.PublicAddr, "{babyUid}", babyUID).Replace(tpl)
	}

	return ""
}

// createTokenProvider creates a function that provides fresh auth tokens
// This is called by the remote relay when it needs to reconnect
func (app *App) createTokenProvider() stream.TokenProvider {
	return func() (string, error) {
		// Force re-authorization to get a fresh token
		if err := app.RestClient.MaybeAuthorize(true); err != nil {
			log.Error().Err(err).Msg("Failed to refresh auth token for remote stream")
			return "", err
		}
		return app.SessionStore.GetAuthToken(), nil
	}
}

// onAuthFailed is called when authentication fails and cannot be recovered
// This typically happens when the refresh token expires and 2FA is enabled
func (app *App) onAuthFailed(err error) {
	log.Error().
		Err(err).
		Msg("Authentication failed - remote streaming unavailable. " +
			"If you have 2FA enabled, you may need to get a new refresh token manually. " +
			"See documentation for instructions on obtaining a refresh token.")
}

// startNotificationPolling starts the single message notification polling loop.
// Message polling only; no sleep/statistics subsystem.
func (app *App) startNotificationPolling(ctx utils.GracefulContext) {
	pollInterval := app.Opts.Notifications.PollInterval
	if pollInterval == 0 {
		pollInterval = app.Opts.EventPolling.PollingInterval
	}
	msgTimeout := app.Opts.Notifications.MessageTimeout
	if msgTimeout == 0 {
		msgTimeout = app.Opts.EventPolling.MessageTimeout
	}
	pollerConfig := notification.PollerConfig{
		PollInterval:   pollInterval,
		Jitter:         app.Opts.Notifications.Jitter,
		MaxBackoff:     app.Opts.Notifications.MaxBackoff,
		MessageTimeout: msgTimeout,
		MessageLimit:   50,
		DedupMaxSize:   1000,
	}

	if pollerConfig.PollInterval == 0 {
		pollerConfig.PollInterval = 30 * time.Second
	}
	if pollerConfig.Jitter == 0 {
		pollerConfig.Jitter = 0.3
	}
	if pollerConfig.MaxBackoff == 0 {
		pollerConfig.MaxBackoff = 5 * time.Minute
	}
	if pollerConfig.MessageTimeout == 0 {
		pollerConfig.MessageTimeout = 5 * time.Minute
	}

	managerConfig := notification.ManagerConfig{
		PollerConfig: pollerConfig,
		TopicPrefix:  app.MQTTConnection.GetTopicPrefix(),
		Babies:       app.SessionStore.GetBabies(),
	}

	// Synchronous readiness: the adapter must never capture nil even if Run
	// has not executed its async initialization yet.
	client := app.MQTTConnection.EnsureClient()
	mqttAdapter := notification.NewPahoMQTTAdapter(client)

	app.notificationManager = notification.NewManager(managerConfig, app.RestClient, mqttAdapter)
	// Single writer for motion/sound state: funnel notification motion/sound
	// events through the MQTT Connection's WColan-compatible publication.
	if app.MQTTConnection != nil {
		mqttConn := app.MQTTConnection
		app.notificationManager.OnEvent = func(ev notification.Event) {
			switch ev.Type {
			case notification.NotificationMotion:
				mqttConn.PublishMotionSoundEvent(ev.BabyUID, "motion", int32(ev.Timestamp.Unix()))
			case notification.NotificationSound:
				mqttConn.PublishMotionSoundEvent(ev.BabyUID, "sound", int32(ev.Timestamp.Unix()))
			}
		}
	}
	log.Info().
		Int("babies", len(managerConfig.Babies)).
		Dur("poll_interval", pollerConfig.PollInterval).
		Float64("jitter", pollerConfig.Jitter).
		Msg("Starting notification event polling")

	ctx.RunAsChild(func(childCtx utils.GracefulContext) {
		stdCtx, cancel := context.WithCancel(context.Background())
		go func() {
			<-childCtx.Done()
			cancel()
		}()

		if err := app.notificationManager.Run(stdCtx); err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("Notification polling stopped with error")
		}
	})
}
