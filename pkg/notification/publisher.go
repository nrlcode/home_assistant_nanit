package notification

import (
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
)

// MQTTClient interface for publishing to MQTT
type MQTTClient interface {
	Publish(topic string, qos byte, retained bool, payload interface{}) error
}

// Publisher publishes notification events to MQTT
type Publisher struct {
	client      MQTTClient
	topicPrefix string
}

// NewPublisher creates a new notification publisher
func NewPublisher(client MQTTClient, topicPrefix string) *Publisher {
	return &Publisher{
		client:      client,
		topicPrefix: topicPrefix,
	}
}

// EventPayload is the JSON structure published to event topics
type EventPayload struct {
	ID            int    `json:"id"`
	Type          string `json:"type"`
	BabyUID       string `json:"baby_uid"`
	EventUID      string `json:"event_uid,omitempty"`
	Timestamp     string `json:"timestamp"`
	UnixTimestamp int64  `json:"unix_timestamp"`
}

// PublishEvent publishes an event to the appropriate MQTT topics
func (p *Publisher) PublishEvent(event Event) error {
	log.Debug().
		Str("type", string(event.Type)).
		Str("baby_uid", event.BabyUID).
		Str("state_topic", event.StateTopic()).
		Bool("is_boolean", event.IsBooleanState()).
		Msg("Publishing event to MQTT")

	// Publish to event topic
	if err := p.publishEventTopic(event); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	// Publish to state topic
	if err := p.publishStateTopic(event); err != nil {
		return fmt.Errorf("failed to publish state: %w", err)
	}

	log.Debug().
		Str("type", string(event.Type)).
		Str("baby_uid", event.BabyUID).
		Msg("Successfully published event to MQTT")

	return nil
}

// publishEventTopic publishes the full event to the event topic
// Topic: {prefix}/babies/{baby_uid}/events/{event_type}
func (p *Publisher) publishEventTopic(event Event) error {
	topic := fmt.Sprintf("%s/babies/%s/events/%s",
		p.topicPrefix, event.BabyUID, event.MQTTTopic())

	payload := EventPayload{
		ID:            event.ID,
		Type:          string(event.Type),
		BabyUID:       event.BabyUID,
		EventUID:      event.EventUID,
		Timestamp:     event.ISOTimestamp(),
		UnixTimestamp: event.UnixTimestamp(),
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	log.Debug().
		Str("topic", topic).
		Str("type", string(event.Type)).
		Msg("Publishing event")

	return p.client.Publish(topic, 0, false, string(jsonPayload))
}

// publishStateTopic publishes the state value to the state topic
// Topic: {prefix}/babies/{baby_uid}/{state_key}
//
// Motion/sound state is owned exclusively by mqtt.Connection.publishEvent
// (WColan-compatible motion/sound + _active topics with 45s generation-guarded
// windows). To keep exactly one writer per motion/sound state topic, this
// publisher emits only the event JSON for MOTION/SOUND and skips the state
// topic here; the notification Manager funnels those events through the
// Connection callback.
func (p *Publisher) publishStateTopic(event Event) error {
	if event.Type == NotificationMotion || event.Type == NotificationSound {
		return nil
	}
	stateTopic := event.StateTopic()
	if stateTopic == "" {
		return nil // No state topic for this event type
	}

	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, event.BabyUID, stateTopic)

	var payload string
	if event.IsBooleanState() {
		// Boolean state (true/false)
		if event.StateValue() == true {
			payload = "true"
		} else {
			payload = "false"
		}
	} else {
		// Timestamp state - use ISO 8601 format for Home Assistant device_class: timestamp
		payload = event.ISOTimestamp()
	}

	log.Info().
		Str("topic", topic).
		Str("payload", payload).
		Str("event_type", string(event.Type)).
		Msg("Publishing state to MQTT")

	// Use retained=true so Home Assistant can read the last value on startup/reconnect
	err := p.client.Publish(topic, 0, true, payload)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("Failed to publish state to MQTT")
	}
	return err
}

// PublishTimestamp publishes just the event timestamp to a state topic
// Useful for updating motion_timestamp or sound_timestamp
func (p *Publisher) PublishTimestamp(event Event, topicSuffix string) error {
	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, event.BabyUID, topicSuffix)

	// Use ISO 8601 format for Home Assistant device_class: timestamp
	payload := event.ISOTimestamp()

	// Use retained=true so Home Assistant can read the last value
	return p.client.Publish(topic, 0, true, payload)
}

// PublishBoolean publishes a boolean value to a state topic
func (p *Publisher) PublishBoolean(babyUID string, topicSuffix string, value bool) error {
	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, babyUID, topicSuffix)

	payload := "false"
	if value {
		payload = "true"
	}

	// Use retained=true so Home Assistant can read the last value
	return p.client.Publish(topic, 0, true, payload)
}
