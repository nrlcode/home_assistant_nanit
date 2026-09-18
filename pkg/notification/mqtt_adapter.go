package notification

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// PahoMQTTAdapter adapts paho MQTT client to our MQTTClient interface
type PahoMQTTAdapter struct {
	client MQTT.Client
}

// NewPahoMQTTAdapter creates a new adapter for paho MQTT client
func NewPahoMQTTAdapter(client MQTT.Client) *PahoMQTTAdapter {
	return &PahoMQTTAdapter{client: client}
}

// Publish publishes a message to the given topic
func (a *PahoMQTTAdapter) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	token := a.client.Publish(topic, qos, retained, payload)
	token.Wait()
	return token.Error()
}
