package notification

import (
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

// Event represents a parsed notification event
type Event struct {
	// ID is the unique message ID from the API
	ID int

	// Type is the notification type
	Type NotificationType

	// BabyUID is the baby/camera unique identifier
	BabyUID string

	// Timestamp is when the event occurred
	Timestamp time.Time

	// EventUID is the unique event identifier (from data.event.uid)
	EventUID string

	// Data is the raw data field from the message
	Data interface{}
}

// ParseMessage converts a message.Message to an Event
func ParseMessage(msg message.Message) (*Event, error) {
	event := &Event{
		ID:        msg.Id,
		Type:      NotificationType(msg.Type),
		BabyUID:   msg.BabyUid,
		Timestamp: msg.Time.Time(),
		Data:      msg.Data,
	}

	// Extract event UID from data.event.uid if present
	event.EventUID = extractEventUID(msg.Data)

	return event, nil
}

// extractEventUID extracts the event UID from the data field
// The structure is typically: {"event": {"uid": "..."}}
func extractEventUID(data interface{}) string {
	if data == nil {
		return ""
	}

	// Try to get data as map
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return ""
	}

	// Try to get event field
	eventField, ok := dataMap["event"]
	if !ok {
		return ""
	}

	// Try to get event as map
	eventMap, ok := eventField.(map[string]interface{})
	if !ok {
		return ""
	}

	// Try to get uid field
	uid, ok := eventMap["uid"]
	if !ok {
		return ""
	}

	// Convert to string
	uidStr, ok := uid.(string)
	if !ok {
		return ""
	}

	return uidStr
}

// UnixTimestamp returns the event timestamp as Unix seconds
func (e *Event) UnixTimestamp() int64 {
	return e.Timestamp.Unix()
}

// ISOTimestamp returns the event timestamp in ISO 8601 format
func (e *Event) ISOTimestamp() string {
	return e.Timestamp.UTC().Format(time.RFC3339)
}

// Description returns the human-readable description of this event type
func (e *Event) Description() string {
	return e.Type.Description()
}

// MQTTTopic returns the MQTT event topic suffix for this event
func (e *Event) MQTTTopic() string {
	return e.Type.MQTTTopic()
}

// StateTopic returns the MQTT state topic suffix for this event
func (e *Event) StateTopic() string {
	return e.Type.StateTopic()
}

// IsBooleanState returns true if this event publishes a boolean state
func (e *Event) IsBooleanState() bool {
	return e.Type.IsBooleanState()
}

// StateValue returns the state value to publish for this event
func (e *Event) StateValue() interface{} {
	return e.Type.StateValue()
}
