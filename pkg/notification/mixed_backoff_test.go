package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

type babyRouterFetcher struct {
	byBaby map[string]func() ([]message.Message, error)
}

func (f *babyRouterFetcher) FetchMessages(babyUID string, limit int) ([]message.Message, error) {
	return nil, errors.New("legacy path must not be used")
}

func (f *babyRouterFetcher) FetchNewMessages(babyUID string, timeout time.Duration) []message.Message {
	msgs, err := f.FetchNewMessagesCtx(context.Background(), babyUID, timeout)
	if err != nil {
		return nil
	}
	return msgs
}

func (f *babyRouterFetcher) FetchNewMessagesCtx(ctx context.Context, babyUID string, timeout time.Duration) ([]message.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if fn, ok := f.byBaby[babyUID]; ok {
		return fn()
	}
	return nil, errors.New("unknown baby")
}

// Mixed outcomes must not erase failure state; cancellation covers the path.
func TestManagerMixedBabyBackoffIndependent(t *testing.T) {
	now := time.Now()
	okMsg := message.Message{Id: 1, BabyUid: "baby2", Type: "MOTION", Time: message.UnixTime(now)}
	fetcher := &babyRouterFetcher{byBaby: map[string]func() ([]message.Message, error){
		"baby1": func() ([]message.Message, error) { return nil, errors.New("baby1 down") },
		"baby2": func() ([]message.Message, error) { return []message.Message{okMsg}, nil },
	}}
	mqttClient := NewMockMQTTClient()
	config := ManagerConfig{
		PollerConfig: PollerConfig{PollInterval: 10 * time.Millisecond, Jitter: 0, MaxBackoff: time.Minute, MessageTimeout: 5 * time.Minute, DedupMaxSize: 100},
		TopicPrefix:  "nanit",
		Babies:       []baby.Baby{{UID: "baby1", Name: "One"}, {UID: "baby2", Name: "Two"}},
	}
	m := NewManager(config, fetcher, mqttClient)

	if err := m.pollBaby(context.Background(), baby.Baby{UID: "baby1", Name: "One"}); err == nil {
		t.Fatal("baby1 poll should fail")
	}
	afterFail := m.poller.CurrentBackoff()
	if afterFail <= 0 {
		t.Fatal("failing baby should advance backoff")
	}
	if err := m.pollBaby(context.Background(), baby.Baby{UID: "baby2", Name: "Two"}); err != nil {
		t.Fatalf("baby2 poll should succeed: %v", err)
	}
	if got := m.poller.CurrentBackoff(); got != afterFail {
		t.Fatalf("mixed success erased failure backoff: got %v want %v", got, afterFail)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.pollBaby(ctx, baby.Baby{UID: "baby1"}); err == nil {
		t.Fatal("cancelled poll should return error")
	}
}
