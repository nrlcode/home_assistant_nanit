package notification

import (
	"context"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

func TestManager_Run_PollsAndPublishes(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
		},
	}

	mqttClient := NewMockMQTTClient()

	config := ManagerConfig{
		PollerConfig: DefaultPollerConfig(),
		TopicPrefix:  "nanit",
		Babies: []baby.Baby{
			{UID: "baby1", Name: "Test Baby"},
		},
	}

	manager := NewManager(config, fetcher, mqttClient)

	// Run for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	manager.Run(ctx)

	// Verify event was published
	eventTopic := "nanit/babies/baby1/events/motion"
	_, ok := mqttClient.GetPublished(eventTopic)
	if !ok {
		t.Errorf("Event not published to %s", eventTopic)
	}

	// Motion/sound state is owned by mqtt.Connection (single writer); the
	// publisher emits only the event JSON here.
	for _, skipped := range []string{
		"nanit/babies/baby1/motion_timestamp",
		"nanit/babies/baby1/motion",
	} {
		if _, ok := mqttClient.GetPublished(skipped); ok {
			t.Errorf("Publisher must not directly publish motion state to %s", skipped)
		}
	}
}

func TestManager_Run_MultipleBabies(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
		},
	}

	mqttClient := NewMockMQTTClient()

	config := ManagerConfig{
		PollerConfig: DefaultPollerConfig(),
		TopicPrefix:  "nanit",
		Babies: []baby.Baby{
			{UID: "baby1", Name: "Baby One"},
			{UID: "baby2", Name: "Baby Two"},
		},
	}

	manager := NewManager(config, fetcher, mqttClient)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	manager.Run(ctx)

	// Both babies should have been polled
	// (same events for simplicity, but in real use would be different)
	stats := manager.Stats()
	if stats.PollCount < 2 {
		t.Errorf("PollCount = %d, want >= 2 (one per baby)", stats.PollCount)
	}
}

func TestManager_Stats(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
			{Id: 2, BabyUid: "baby1", Type: "SOUND", Time: unixTime},
		},
	}

	mqttClient := NewMockMQTTClient()

	config := ManagerConfig{
		PollerConfig: DefaultPollerConfig(),
		TopicPrefix:  "nanit",
		Babies: []baby.Baby{
			{UID: "baby1", Name: "Test Baby"},
		},
	}

	manager := NewManager(config, fetcher, mqttClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	manager.Run(ctx)

	stats := manager.Stats()
	if stats.PollCount == 0 {
		t.Error("PollCount should be > 0")
	}

	if stats.EventsReceived == 0 {
		t.Error("EventsReceived should be > 0")
	}
}

func TestManager_NoBabies(t *testing.T) {
	fetcher := &MockMessageFetcher{}
	mqttClient := NewMockMQTTClient()

	config := ManagerConfig{
		PollerConfig: DefaultPollerConfig(),
		TopicPrefix:  "nanit",
		Babies:       []baby.Baby{}, // No babies
	}

	manager := NewManager(config, fetcher, mqttClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := manager.Run(ctx)
	if err != nil {
		t.Errorf("Run() error = %v, want nil for no babies", err)
	}
}
