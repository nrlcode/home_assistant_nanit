package main

import (
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/app"
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// validatePort checks if a port string is a valid port number (1-65535)
func validatePort(portStr string) bool {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}
	return port >= 1 && port <= 65535
}

func main() {
	initLogger()
	logAppVersion()
	utils.LoadDotEnvFile()
	setLogLevel()

	opts := app.Opts{
		NanitCredentials: app.NanitCredentials{
			Email:        utils.EnvVarStr("NANIT_EMAIL", ""),
			Password:     utils.EnvVarStr("NANIT_PASSWORD", ""),
			RefreshToken: utils.EnvVarStr("NANIT_REFRESH_TOKEN", ""),
		},
		SessionFile:     utils.EnvVarStr("NANIT_SESSION_FILE", "/data/session.json"),
		DataDirectories: ensureDataDirectories(),
		HTTPEnabled:     utils.EnvVarBool("NANIT_HTTP_ENABLED", false),
		EventPolling: app.EventPollingOpts{
			Enabled: utils.EnvVarBool("NANIT_EVENTS_POLLING", false),
			// 30 second default polling interval
			PollingInterval: utils.EnvVarSeconds("NANIT_EVENTS_POLLING_INTERVAL", 30*time.Second),
			// 300 second (5 min) default message timeout (unseen messages are ignored once they are this old)
			MessageTimeout: utils.EnvVarSeconds("NANIT_EVENTS_MESSAGE_TIMEOUT", 300*time.Second),
		},
	}

	if utils.EnvVarBool("NANIT_RTMP_ENABLED", true) {
		// Explicit host:port is required; empty means sensors-only via the
		// add-on (which sets NANIT_RTMP_ENABLED=false). Never guess a
		// container IP.
		publicAddr := utils.EnvVarStr("NANIT_RTMP_ADDR", "")
		if publicAddr == "" {
			log.Fatal().Msg("NANIT_RTMP_ADDR is required when RTMP is enabled (explicit host:port, e.g. 192.168.1.100:1935). Disable RTMP for sensors-only.")
		}
		m := regexp.MustCompile(":([0-9]+)$").FindStringSubmatch(publicAddr)
		if len(m) != 2 {
			log.Fatal().Msg("Invalid NANIT_RTMP_ADDR. Must include port (e.g., 192.168.1.100:1935)")
		}
		if !validatePort(m[1]) {
			log.Fatal().Str("port", m[1]).Msg("Invalid port in NANIT_RTMP_ADDR. Must be 1-65535")
		}
		listenAddr := ":" + m[1]

		preferRemote := utils.EnvVarBool("NANIT_PREFER_REMOTE", false)

		// Security: IP-based access control
		allowedPresets := utils.EnvVarStr("NANIT_RTMP_ALLOWED_PRESETS", "")
		allowedIPs := utils.EnvVarStr("NANIT_RTMP_ALLOWED_IPS", "")
		logDenied := utils.EnvVarBool("NANIT_RTMP_LOG_DENIED", true)
		logAllowed := utils.EnvVarBool("NANIT_RTMP_LOG_ALLOWED", false)

		if allowedPresets != "" || allowedIPs != "" {
			log.Info().
				Str("presets", allowedPresets).
				Str("allowed_ips", allowedIPs).
				Bool("log_denied", logDenied).
				Bool("log_allowed", logAllowed).
				Msg("RTMP security configuration")
		} else {
			log.Warn().Msg("No RTMP security configured - set NANIT_RTMP_ALLOWED_PRESETS or NANIT_RTMP_ALLOWED_IPS")
		}

		log.Info().Bool("prefer_remote", preferRemote).Str("public_addr", publicAddr).Msg("RTMP streaming configuration")

		opts.RTMP = &app.RTMPOpts{
			ListenAddr:     listenAddr,
			PublicAddr:     publicAddr,
			PreferRemote:   preferRemote,
			AllowedPresets: allowedPresets,
			AllowedIPs:     allowedIPs,
			LogDenied:      logDenied,
			LogAllowed:     logAllowed,
		}
	}

	if utils.EnvVarBool("NANIT_MQTT_ENABLED", false) {
		rtmpAddr := ""
		if opts.RTMP != nil {
			rtmpAddr = opts.RTMP.PublicAddr
		}

		opts.MQTT = &mqtt.Opts{
			BrokerURL:        utils.EnvVarReqStr("NANIT_MQTT_BROKER_URL"),
			ClientID:         utils.EnvVarStr("NANIT_MQTT_CLIENT_ID", "nanit"),
			Username:         utils.EnvVarStr("NANIT_MQTT_USERNAME", ""),
			Password:         utils.EnvVarStr("NANIT_MQTT_PASSWORD", ""),
			TopicPrefix:      utils.EnvVarStr("NANIT_MQTT_PREFIX", "nanit"),
			DiscoveryEnabled: utils.EnvVarBool("NANIT_MQTT_DISCOVERY", true),
			DiscoveryPrefix:  utils.EnvVarStr("NANIT_MQTT_DISCOVERY_PREFIX", "homeassistant"),
			RTMPAddr:         rtmpAddr,
		}

		if opts.MQTT.DiscoveryEnabled {
			log.Info().Msg("Home Assistant MQTT discovery enabled")
		}

		// Single message polling loop, gated by the existing events switch.
		// No separate notification/sleep controls.
		if opts.EventPolling.Enabled {
			opts.Notifications = app.NotificationOpts{
				Enabled:        true,
				PollInterval:   opts.EventPolling.PollingInterval,
				Jitter:         0.3,
				MaxBackoff:     5 * time.Minute,
				MessageTimeout: opts.EventPolling.MessageTimeout,
			}
			log.Info().
				Dur("poll_interval", opts.Notifications.PollInterval).
				Float64("jitter", opts.Notifications.Jitter).
				Msg("Message notification polling enabled")
		}
	}

	if opts.EventPolling.Enabled {
		log.Info().Msgf("Event polling enabled with an interval of %v", opts.EventPolling.PollingInterval)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	instance, err := app.NewApp(opts)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application")
	}

	runner := utils.RunWithGracefulCancel(instance.Run)

	<-interrupt
	log.Warn().Msg("Received interrupt signal, terminating")

	waitForCleanup := make(chan struct{}, 1)

	go func() {
		runner.Cancel()
		close(waitForCleanup)
	}()

	select {
	case <-interrupt:
		log.Fatal().Msg("Received another interrupt signal, forcing termination without clean up")
	case <-waitForCleanup:
		log.Info().Msg("Clean exit")
		return
	}
}
