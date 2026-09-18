package notification

import (
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

func TestParseMessage(t *testing.T) {
	// Create a sample message with Unix timestamp
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	msg := message.Message{
		Id:      12345,
		BabyUid: "baby123",
		Type:    "MOTION",
		Time:    unixTime,
		Data: map[string]interface{}{
			"event": map[string]interface{}{
				"uid": "event-abc-123",
			},
		},
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	if event.ID != 12345 {
		t.Errorf("ID = %d, want 12345", event.ID)
	}

	if event.Type != NotificationMotion {
		t.Errorf("Type = %v, want %v", event.Type, NotificationMotion)
	}

	if event.BabyUID != "baby123" {
		t.Errorf("BabyUID = %q, want %q", event.BabyUID, "baby123")
	}

	if !event.Timestamp.Equal(timestamp) {
		t.Errorf("Timestamp = %v, want %v", event.Timestamp, timestamp)
	}

	if event.EventUID != "event-abc-123" {
		t.Errorf("EventUID = %q, want %q", event.EventUID, "event-abc-123")
	}
}

func TestParseMessageTemperature(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	msg := message.Message{
		Id:      200,
		BabyUid: "baby789",
		Type:    "TEMPERATURE",
		Time:    unixTime,
		Data: map[string]interface{}{
			"temperature": 28.5,
		},
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	if event.Type != NotificationTemperature {
		t.Errorf("Type = %v, want %v", event.Type, NotificationTemperature)
	}
}

func TestParseMessageCameraOffline(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	msg := message.Message{
		Id:      300,
		BabyUid: "baby-offline",
		Type:    "CAMERA_OFFLINE",
		Time:    unixTime,
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	if event.Type != NotificationCameraOffline {
		t.Errorf("Type = %v, want %v", event.Type, NotificationCameraOffline)
	}
}

func TestParseMessageUnknownType(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	msg := message.Message{
		Id:      400,
		BabyUid: "baby-unknown",
		Type:    "SOME_NEW_TYPE",
		Time:    unixTime,
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	// Should still parse, just with unknown type
	if event.Type != NotificationType("SOME_NEW_TYPE") {
		t.Errorf("Type = %v, want %v", event.Type, NotificationType("SOME_NEW_TYPE"))
	}
}

func TestParseMessageMissingEventUID(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	msg := message.Message{
		Id:      500,
		BabyUid: "baby-no-event",
		Type:    "MOTION",
		Time:    unixTime,
		Data:    nil, // No data field
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	// Should still parse, EventUID will be empty
	if event.EventUID != "" {
		t.Errorf("EventUID = %q, want empty string", event.EventUID)
	}
}

func TestParseMessageNestedEventUID(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	// Test with nested map structure (common in JSON responses)
	msg := message.Message{
		Id:      600,
		BabyUid: "baby-nested",
		Type:    "MOTION",
		Time:    unixTime,
		Data: map[string]interface{}{
			"event": map[string]interface{}{
				"uid":  "nested-event-uid",
				"type": "motion",
			},
		},
	}

	event, err := ParseMessage(msg)
	if err != nil {
		t.Fatalf("ParseMessage() error = %v", err)
	}

	if event.EventUID != "nested-event-uid" {
		t.Errorf("EventUID = %q, want %q", event.EventUID, "nested-event-uid")
	}
}

func TestEventUnixTimestamp(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	event := Event{
		ID:        1,
		Type:      NotificationMotion,
		BabyUID:   "baby",
		Timestamp: timestamp,
	}

	got := event.UnixTimestamp()
	want := timestamp.Unix()

	if got != want {
		t.Errorf("UnixTimestamp() = %d, want %d", got, want)
	}
}

func TestEventISOTimestamp(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	event := Event{
		ID:        1,
		Type:      NotificationMotion,
		BabyUID:   "baby",
		Timestamp: timestamp,
	}

	got := event.ISOTimestamp()
	want := "2024-01-15T10:30:00Z"

	if got != want {
		t.Errorf("ISOTimestamp() = %q, want %q", got, want)
	}
}

func TestAllNotificationTypesParseable(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	unixTime := message.UnixTime(timestamp)

	// Test that all notification types can be parsed
	types := []string{
		"MOTION",
		"SOUND",
		"BABY_STANDING",
		"TODDLER_LEFT_THE_BED",
		"ALERT_ZONE",
		"TEMPERATURE",
		"HUMIDITY",
		"BREATHING_ALERT",
		"CAMERA_OFFLINE",
		"CAMERA_ONLINE",
		"LOW_BATTERY",
		"CHANGE_STATE",
	}

	for _, msgType := range types {
		t.Run(msgType, func(t *testing.T) {
			msg := message.Message{
				Id:      1,
				BabyUid: "baby",
				Type:    msgType,
				Time:    unixTime,
			}

			event, err := ParseMessage(msg)
			if err != nil {
				t.Fatalf("ParseMessage(%s) error = %v", msgType, err)
			}

			if string(event.Type) != msgType {
				t.Errorf("Type = %v, want %v", event.Type, msgType)
			}
		})
	}
}
