package notification

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPublishSleepTimelineForDateValidationAndSelection(t *testing.T) {
	client := NewMockMQTTClient()
	p := NewPublisher(client, "nanit")
	date := time.Now().UTC().Format("2006-01-02")
	history := &historyFile{HistoryStatus: "ok", Reports: []historyReport{{Key: date + "||night", Revision: 3}}}
	if err := p.PublishSleepTimelineForDate("baby", date, history); err != nil {
		t.Fatal(err)
	}
	if got, _ := client.GetPublished("nanit/babies/baby/sleep_timeline_history"); got != date+"||night" {
		t.Fatalf("selected=%q", got)
	}
	for _, invalid := range []string{"", "2026-9-18", "2026-02-30", time.Now().UTC().Add(24 * time.Hour).Format("2006-01-02")} {
		client = NewMockMQTTClient()
		if err := NewPublisher(client, "nanit").PublishSleepTimelineForDate("baby", invalid, history); err != nil {
			t.Fatal(err)
		}
		if got, _ := client.GetPublished("nanit/babies/baby/sleep_timeline_history/availability"); got != "offline" {
			t.Fatalf("date=%q availability=%q", invalid, got)
		}
	}
}

func TestPublishSleepTimelineForDateMissingAndPruned(t *testing.T) {
	for _, history := range []*historyFile{nil, {HistoryStatus: "ok", Reports: []historyReport{{Key: "2020-01-01||night"}}}} {
		client := NewMockMQTTClient()
		if err := NewPublisher(client, "nanit").PublishSleepTimelineForDate("baby", "2024-01-01", history); err != nil {
			t.Fatal(err)
		}
		if got, _ := client.GetPublished("nanit/babies/baby/sleep_timeline_history/availability"); got != "offline" {
			t.Fatalf("availability=%q", got)
		}
	}
}

func TestPublishSleepTimelineForDateCorrectionAndBabyIsolation(t *testing.T) {
	client := NewMockMQTTClient()
	history := &historyFile{HistoryStatus: "ok", Reports: []historyReport{{Key: "2024-01-01||night", Revision: 1}, {Key: "2024-01-01||night", Revision: 2}}}
	p := NewPublisher(client, "nanit")
	if err := p.PublishSleepTimelineForDate("a", "2024-01-01", history); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishSleepTimelineForDate("b", "2024-01-01", &historyFile{HistoryStatus: "ok", Reports: []historyReport{{Key: "2024-01-01||nap"}}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := client.GetPublished("nanit/babies/a/sleep_timeline_history"); got != "2024-01-01||night" {
		t.Fatalf("a=%q", got)
	}
	if got, _ := client.GetPublished("nanit/babies/b/sleep_timeline_history"); got != "2024-01-01||nap" {
		t.Fatalf("b=%q", got)
	}
}

type failingTimelineClient struct{ MockMQTTClient }

func (f *failingTimelineClient) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	if strings.HasSuffix(topic, "_attributes") {
		return errors.New("publish failed")
	}
	return f.MockMQTTClient.Publish(topic, qos, retained, payload)
}

func TestPublishSleepTimelineForDatePropagatesFailure(t *testing.T) {
	client := &failingTimelineClient{MockMQTTClient: *NewMockMQTTClient()}
	history := &historyFile{HistoryStatus: "ok", Reports: []historyReport{{Key: "2024-01-01||night"}}}
	if err := NewPublisher(client, "nanit").PublishSleepTimelineForDate("baby", "2024-01-01", history); err == nil {
		t.Fatal("expected publish error")
	}
}

// MockMQTTClient implements MQTTClient for testing
type MockMQTTClient struct {
	mu        sync.Mutex
	published map[string]string // topic -> payload
}

func NewMockMQTTClient() *MockMQTTClient {
	return &MockMQTTClient{
		published: make(map[string]string),
	}
}

func (m *MockMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var payloadStr string
	switch v := payload.(type) {
	case string:
		payloadStr = v
	case []byte:
		payloadStr = string(v)
	default:
		b, _ := json.Marshal(v)
		payloadStr = string(b)
	}

	m.published[topic] = payloadStr
	return nil
}

func (m *MockMQTTClient) GetPublished(topic string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.published[topic]
	return v, ok
}

func (m *MockMQTTClient) AllPublished() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]string)
	for k, v := range m.published {
		result[k] = v
	}
	return result
}

func TestPublisherPublishEvent_Motion(t *testing.T) {
	client := NewMockMQTTClient()
	publisher := NewPublisher(client, "nanit")

	event := Event{
		ID:        1,
		Type:      NotificationMotion,
		BabyUID:   "baby123",
		Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		EventUID:  "event-abc",
	}

	err := publisher.PublishEvent(event)
	if err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}

	// Check event topic
	eventTopic := "nanit/babies/baby123/events/motion"
	payload, ok := client.GetPublished(eventTopic)
	if !ok {
		t.Errorf("Event not published to %s", eventTopic)
	}

	// Verify payload contains expected fields
	var eventData map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &eventData); err != nil {
		t.Fatalf("Failed to parse payload: %v", err)
	}

	if eventData["event_uid"] != "event-abc" {
		t.Errorf("event_uid = %v, want event-abc", eventData["event_uid"])
	}

	// Check state topic: motion/sound state is owned by mqtt.Connection
	// (single writer), so the notification publisher must NOT publish it
	// directly. The Manager funnels these through the Connection callback.
	for _, skipped := range []string{
		"nanit/babies/baby123/motion_timestamp",
		"nanit/babies/baby123/motion",
		"nanit/babies/baby123/motion_active",
	} {
		if _, ok := client.GetPublished(skipped); ok {
			t.Errorf("Publisher must not directly publish motion state to %s (Connection owns it)", skipped)
		}
	}
}

func TestPublisherPublishEvent_CameraOffline(t *testing.T) {
	client := NewMockMQTTClient()
	publisher := NewPublisher(client, "nanit")

	event := Event{
		ID:        3,
		Type:      NotificationCameraOffline,
		BabyUID:   "baby789",
		Timestamp: time.Now(),
	}

	err := publisher.PublishEvent(event)
	if err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}

	// Check state topic (camera_offline sets camera_online to false)
	stateTopic := "nanit/babies/baby789/camera_online"
	statePayload, ok := client.GetPublished(stateTopic)
	if !ok {
		t.Errorf("State not published to %s", stateTopic)
	}

	if statePayload != "false" {
		t.Errorf("camera_online = %s, want false", statePayload)
	}
}

func TestPublisherPublishEvent_CameraOnline(t *testing.T) {
	client := NewMockMQTTClient()
	publisher := NewPublisher(client, "nanit")

	event := Event{
		ID:        4,
		Type:      NotificationCameraOnline,
		BabyUID:   "baby789",
		Timestamp: time.Now(),
	}

	err := publisher.PublishEvent(event)
	if err != nil {
		t.Fatalf("PublishEvent() error = %v", err)
	}

	// Check state topic
	stateTopic := "nanit/babies/baby789/camera_online"
	statePayload, ok := client.GetPublished(stateTopic)
	if !ok {
		t.Errorf("State not published to %s", stateTopic)
	}

	if statePayload != "true" {
		t.Errorf("camera_online = %s, want true", statePayload)
	}
}

func TestPublisherPublishEvent_AllTypes(t *testing.T) {
	types := []struct {
		notifType  NotificationType
		eventTopic string
		stateTopic string
		stateValue string
	}{
		{NotificationMotion, "events/motion", "", ""},
		{NotificationSound, "events/sound", "", ""},
		{NotificationStanding, "events/standing", "is_standing", "true"},
		{NotificationLeftBed, "events/left_bed", "left_bed", "true"},
		{NotificationAlertZone, "events/alert_zone", "alert_zone_triggered", "true"},
		{NotificationTemperature, "events/temperature_alert", "temperature_alert", "true"},
		{NotificationHumidity, "events/humidity_alert", "humidity_alert", "true"},
		{NotificationBreathing, "events/breathing_alert", "breathing_alert", "true"},
		{NotificationCameraOffline, "events/camera_offline", "camera_online", "false"},
		{NotificationCameraOnline, "events/camera_online", "camera_online", "true"},
		{NotificationLowBattery, "events/low_battery", "low_battery", "true"},
	}

	for _, tt := range types {
		t.Run(string(tt.notifType), func(t *testing.T) {
			client := NewMockMQTTClient()
			publisher := NewPublisher(client, "nanit")

			event := Event{
				ID:        1,
				Type:      tt.notifType,
				BabyUID:   "baby",
				Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			}

			err := publisher.PublishEvent(event)
			if err != nil {
				t.Fatalf("PublishEvent() error = %v", err)
			}

			// Check event topic
			eventTopic := "nanit/babies/baby/" + tt.eventTopic
			_, ok := client.GetPublished(eventTopic)
			if !ok {
				t.Errorf("Event not published to %s", eventTopic)
			}

			// Check state topic (if applicable)
			if tt.stateTopic != "" {
				stateTopic := "nanit/babies/baby/" + tt.stateTopic
				statePayload, ok := client.GetPublished(stateTopic)
				if !ok {
					t.Errorf("State not published to %s", stateTopic)
				}

				// For boolean states, verify the value
				if tt.stateValue != "" && statePayload != tt.stateValue {
					t.Errorf("State value = %s, want %s", statePayload, tt.stateValue)
				}
			}
		})
	}
}

func TestPublisherTopicPrefix(t *testing.T) {
	client := NewMockMQTTClient()
	publisher := NewPublisher(client, "home/nanit")

	event := Event{
		ID:        1,
		Type:      NotificationMotion,
		BabyUID:   "baby",
		Timestamp: time.Now(),
	}

	publisher.PublishEvent(event)

	// Should use custom prefix
	eventTopic := "home/nanit/babies/baby/events/motion"
	_, ok := client.GetPublished(eventTopic)
	if !ok {
		t.Errorf("Event not published with custom prefix to %s", eventTopic)
	}
}

func TestPublisherEventPayloadFormat(t *testing.T) {
	client := NewMockMQTTClient()
	publisher := NewPublisher(client, "nanit")

	timestamp := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	event := Event{
		ID:        42,
		Type:      NotificationMotion,
		BabyUID:   "baby",
		Timestamp: timestamp,
		EventUID:  "event-xyz",
	}

	publisher.PublishEvent(event)

	eventTopic := "nanit/babies/baby/events/motion"
	payload, _ := client.GetPublished(eventTopic)

	var eventData map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &eventData); err != nil {
		t.Fatalf("Failed to parse payload: %v", err)
	}

	// Check all expected fields
	if eventData["id"] != float64(42) {
		t.Errorf("id = %v, want 42", eventData["id"])
	}

	if eventData["type"] != "MOTION" {
		t.Errorf("type = %v, want MOTION", eventData["type"])
	}

	if eventData["baby_uid"] != "baby" {
		t.Errorf("baby_uid = %v, want baby", eventData["baby_uid"])
	}

	if eventData["event_uid"] != "event-xyz" {
		t.Errorf("event_uid = %v, want event-xyz", eventData["event_uid"])
	}

	if eventData["timestamp"] != "2024-01-15T10:30:00Z" {
		t.Errorf("timestamp = %v, want 2024-01-15T10:30:00Z", eventData["timestamp"])
	}

	if eventData["unix_timestamp"] != float64(1705314600) {
		t.Errorf("unix_timestamp = %v, want 1705314600", eventData["unix_timestamp"])
	}
}
