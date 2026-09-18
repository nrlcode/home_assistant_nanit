package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

type babyAwareFetcher struct {
	byBaby map[string][]message.Message
	err    error
	block  chan struct{}
}

func (f *babyAwareFetcher) FetchMessages(babyUID string, _ int) ([]message.Message, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byBaby[babyUID], nil
}

func (f *babyAwareFetcher) FetchNewMessages(babyUID string, _ time.Duration) []message.Message {
	return f.byBaby[babyUID]
}

func (f *babyAwareFetcher) FetchNewMessagesCtx(ctx context.Context, babyUID string, _ time.Duration) ([]message.Message, error) {
	if f.block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.block:
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.byBaby[babyUID], nil
}

func TestPollerBabyScopedDedupEqualIDs(t *testing.T) {
	now := time.Now()
	mk := func(id int, uid string, ts time.Time) message.Message {
		return message.Message{Id: id, BabyUid: uid, Type: "MOTION", Time: message.UnixTime(ts)}
	}
	fetcher := &babyAwareFetcher{byBaby: map[string][]message.Message{
		"baby1": {mk(1, "baby1", now.Add(-time.Minute))},
		"baby2": {mk(1, "baby2", now.Add(-30*time.Second))},
	}}
	poller := NewPoller(PollerConfig{
		PollInterval: 10 * time.Millisecond, Jitter: 0,
		MaxBackoff: 5 * time.Minute, MessageTimeout: 5 * time.Minute, DedupMaxSize: 100,
	}, fetcher)
	ctx := context.Background()
	ev1, err := poller.Poll(ctx, "baby1")
	if err != nil {
		t.Fatalf("poll baby1: %v", err)
	}
	if len(ev1) != 1 {
		t.Fatalf("baby1 events = %d, want 1", len(ev1))
	}
	ev2, err := poller.Poll(ctx, "baby2")
	if err != nil {
		t.Fatalf("poll baby2: %v", err)
	}
	if len(ev2) != 1 {
		t.Fatalf("baby2 events = %d, want 1 (same numeric ID must not dedup across babies)", len(ev2))
	}
	// Re-poll baby1 with same ID: now deduped.
	ev1b, err := poller.Poll(ctx, "baby1")
	if err != nil {
		t.Fatalf("re-poll baby1: %v", err)
	}
	if len(ev1b) != 0 {
		t.Fatalf("re-poll baby1 events = %d, want 0 (deduped)", len(ev1b))
	}
}

func TestPollerFetchErrorDrivesBackoffAndRecovery(t *testing.T) {
	now := time.Now()
	fetcher := &babyAwareFetcher{
		err: errors.New("boom 500"),
		byBaby: map[string][]message.Message{
			"baby1": {{Id: 7, BabyUid: "baby1", Type: "MOTION", Time: message.UnixTime(now)}},
		},
	}
	cfg := PollerConfig{
		PollInterval: 10 * time.Millisecond, Jitter: 0,
		MaxBackoff: 5 * time.Minute, MessageTimeout: 5 * time.Minute, DedupMaxSize: 100,
	}
	mgr := NewManager(ManagerConfig{
		PollerConfig: cfg,
		TopicPrefix:  "nanit",
		Babies:       []baby.Baby{{UID: "baby1", Name: "B"}},
	}, fetcher, NewMockMQTTClient())
	ctx := context.Background()
	var last time.Duration
	for i := 0; i < 5; i++ {
		if err := mgr.pollBaby(ctx, baby.Baby{UID: "baby1", Name: "B"}); err == nil {
			t.Fatalf("pollBaby %d should fail", i)
		}
		got := mgr.poller.CurrentBackoff()
		if i > 0 && got < last {
			t.Fatalf("backoff decreased: %v -> %v", last, got)
		}
		last = got
	}
	if last > 5*time.Minute {
		t.Fatalf("backoff %v exceeds cap 5m", last)
	}
	// Recovery: clear error, successful poll resets backoff.
	fetcher.err = nil
	if err := mgr.pollBaby(ctx, baby.Baby{UID: "baby1", Name: "B"}); err != nil {
		t.Fatalf("recovery pollBaby: %v", err)
	}
	if got := mgr.poller.CurrentBackoff(); got != 0 {
		t.Fatalf("after recovery backoff = %v, want 0", got)
	}
}

func TestPollerCancellationDuringInflightFetch(t *testing.T) {
	fetcher := &babyAwareFetcher{
		block:  make(chan struct{}),
		byBaby: map[string][]message.Message{},
	}
	poller := NewPoller(PollerConfig{
		PollInterval: 10 * time.Millisecond, Jitter: 0,
		MaxBackoff: 5 * time.Minute, MessageTimeout: 5 * time.Minute, DedupMaxSize: 100,
	}, fetcher)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := poller.Poll(ctx, "baby1")
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight Poll err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled in-flight Poll did not return")
	}
}
