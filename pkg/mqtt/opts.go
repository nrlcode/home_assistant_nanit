package mqtt

// Opts - holds configuration needed to establish connection to the broker
type Opts struct {
	BrokerURL string
	ClientID  string

	Username string
	Password string

	TopicPrefix string

	// Home Assistant MQTT discovery
	DiscoveryEnabled bool
	DiscoveryPrefix  string // usually "homeassistant"

	// RTMPAddr is the explicit advertised host:port for stream_url (empty = sensors-only)
	RTMPAddr string
}
