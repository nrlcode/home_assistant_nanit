package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

// MockMessageFetcher implements MessageFetcher for testing
type MockMessageFetcher struct {
	messages []message.Message
	err      error
	calls    int
}

func (m *MockMessageFetcher) FetchMessages(babyUID string, limit int) ([]message.Message, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	return m.messages, nil
}

func (m *MockMessageFetcher) FetchNewMessages(babyUID string, timeout time.Duration) []message.Message {
	m.calls++
	return m.messages
}

func (m *MockMessageFetcher) FetchNewMessagesCtx(ctx context.Context, babyUID string, timeout time.Duration) ([]message.Message, error) {
	m.calls++
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.messages, nil
}

func TestPollerConfig_Defaults(t *testing.T) {
	config := DefaultPollerConfig()

	if config.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", config.PollInterval)
	}

	if config.Jitter != 0.3 {
		t.Errorf("Jitter = %v, want 0.3", config.Jitter)
	}

	if config.MaxBackoff != 5*time.Minute {
		t.Errorf("MaxBackoff = %v, want 5m", config.MaxBackoff)
	}

	if config.MessageLimit != 50 {
		t.Errorf("MessageLimit = %d, want 50", config.MessageLimit)
	}

	if config.DedupMaxSize != 1000 {
		t.Errorf("DedupMaxSize = %d, want 1000", config.DedupMaxSize)
	}
}

func TestPollerNextInterval_NoJitter(t *testing.T) {
	config := DefaultPollerConfig()
	config.Jitter = 0 // Disable jitter for deterministic test
	config.PollInterval = 10 * time.Second

	poller := NewPoller(config, nil)

	interval := poller.NextInterval()
	if interval != 10*time.Second {
		t.Errorf("NextInterval() = %v, want 10s", interval)
	}
}

func TestPollerNextInterval_WithJitter(t *testing.T) {
	config := DefaultPollerConfig()
	config.Jitter = 0.3 // ±30%
	config.PollInterval = 10 * time.Second

	poller := NewPoller(config, nil)

	// Run multiple times to check jitter range
	minExpected := 7 * time.Second  // 10 * 0.7
	maxExpected := 13 * time.Second // 10 * 1.3

	for i := 0; i < 100; i++ {
		interval := poller.NextInterval()
		if interval < minExpected || interval > maxExpected {
			t.Errorf("NextInterval() = %v, want between %v and %v", interval, minExpected, maxExpected)
		}
	}
}

func TestPollerBackoff_Exponential(t *testing.T) {
	config := DefaultPollerConfig()
	config.PollInterval = 10 * time.Second
	config.MaxBackoff = 5 * time.Minute
	config.Jitter = 0 // Disable jitter for deterministic test

	poller := NewPoller(config, nil)

	// Initial backoff should be 0
	if poller.CurrentBackoff() != 0 {
		t.Errorf("Initial backoff = %v, want 0", poller.CurrentBackoff())
	}

	// After 1 error: 2^1 * 10s = 20s
	poller.IncrementBackoff()
	expected := 20 * time.Second
	if poller.CurrentBackoff() != expected {
		t.Errorf("Backoff after 1 error = %v, want %v", poller.CurrentBackoff(), expected)
	}

	// After 2 errors: 2^2 * 10s = 40s
	poller.IncrementBackoff()
	expected = 40 * time.Second
	if poller.CurrentBackoff() != expected {
		t.Errorf("Backoff after 2 errors = %v, want %v", poller.CurrentBackoff(), expected)
	}

	// After 3 errors: 2^3 * 10s = 80s
	poller.IncrementBackoff()
	expected = 80 * time.Second
	if poller.CurrentBackoff() != expected {
		t.Errorf("Backoff after 3 errors = %v, want %v", poller.CurrentBackoff(), expected)
	}
}

func TestPollerBackoff_CappedAtMax(t *testing.T) {
	config := DefaultPollerConfig()
	config.PollInterval = 10 * time.Second
	config.MaxBackoff = 60 * time.Second // Cap at 1 minute
	config.Jitter = 0

	poller := NewPoller(config, nil)

	// Increment many times
	for i := 0; i < 10; i++ {
		poller.IncrementBackoff()
	}

	// Should be capped at MaxBackoff
	if poller.CurrentBackoff() > config.MaxBackoff {
		t.Errorf("Backoff = %v, should be capped at %v", poller.CurrentBackoff(), config.MaxBackoff)
	}
}

func TestPollerBackoff_ResetOnSuccess(t *testing.T) {
	config := DefaultPollerConfig()
	config.PollInterval = 10 * time.Second
	config.Jitter = 0

	poller := NewPoller(config, nil)

	// Build up some backoff
	poller.IncrementBackoff()
	poller.IncrementBackoff()

	if poller.CurrentBackoff() == 0 {
		t.Error("Backoff should be non-zero after errors")
	}

	// Reset
	poller.ResetBackoff()

	if poller.CurrentBackoff() != 0 {
		t.Errorf("Backoff = %v, want 0 after reset", poller.CurrentBackoff())
	}

	if poller.ErrorCount() != 0 {
		t.Errorf("ErrorCount = %d, want 0 after reset", poller.ErrorCount())
	}
}

func TestPollerPoll_ReturnsNewEvents(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
			{Id: 2, BabyUid: "baby1", Type: "SOUND", Time: unixTime},
		},
	}

	config := DefaultPollerConfig()
	poller := NewPoller(config, fetcher)

	ctx := context.Background()
	events, err := poller.Poll(ctx, "baby1")

	if err != nil {
		t.Fatalf("Poll() error = %v", err)
	}

	if len(events) != 2 {
		t.Errorf("Poll() returned %d events, want 2", len(events))
	}
}

func TestPollerPoll_DeduplicatesEvents(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
			{Id: 2, BabyUid: "baby1", Type: "SOUND", Time: unixTime},
		},
	}

	config := DefaultPollerConfig()
	poller := NewPoller(config, fetcher)

	ctx := context.Background()

	// First poll should return both events
	events1, _ := poller.Poll(ctx, "baby1")
	if len(events1) != 2 {
		t.Errorf("First poll returned %d events, want 2", len(events1))
	}

	// Second poll with same messages should return 0 (deduplicated)
	events2, _ := poller.Poll(ctx, "baby1")
	if len(events2) != 0 {
		t.Errorf("Second poll returned %d events, want 0 (deduplicated)", len(events2))
	}
}

func TestPollerPoll_NewMessagesAfterDedup(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
		},
	}

	config := DefaultPollerConfig()
	poller := NewPoller(config, fetcher)

	ctx := context.Background()

	// First poll
	events1, _ := poller.Poll(ctx, "baby1")
	if len(events1) != 1 {
		t.Errorf("First poll returned %d events, want 1", len(events1))
	}

	// Add a new message
	fetcher.messages = []message.Message{
		{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime}, // Old
		{Id: 2, BabyUid: "baby1", Type: "CRY", Time: unixTime},    // New
	}

	// Second poll should only return the new message
	events2, _ := poller.Poll(ctx, "baby1")
	if len(events2) != 1 {
		t.Errorf("Second poll returned %d events, want 1", len(events2))
	}

	if events2[0].ID != 2 {
		t.Errorf("Second poll returned event ID %d, want 2", events2[0].ID)
	}
}

func TestPollerPoll_ContextCancellation(t *testing.T) {
	fetcher := &MockMessageFetcher{
		messages: nil,
	}

	config := DefaultPollerConfig()
	poller := NewPoller(config, fetcher)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := poller.Poll(ctx, "baby1")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Poll() error = %v, want context.Canceled", err)
	}
}

func TestPollerStats(t *testing.T) {
	timestamp := time.Now()
	unixTime := message.UnixTime(timestamp)

	fetcher := &MockMessageFetcher{
		messages: []message.Message{
			{Id: 1, BabyUid: "baby1", Type: "MOTION", Time: unixTime},
		},
	}

	config := DefaultPollerConfig()
	poller := NewPoller(config, fetcher)

	ctx := context.Background()

	// Initial stats
	stats := poller.Stats()
	if stats.PollCount != 0 {
		t.Errorf("Initial PollCount = %d, want 0", stats.PollCount)
	}

	// After a poll
	poller.Poll(ctx, "baby1")
	stats = poller.Stats()

	if stats.PollCount != 1 {
		t.Errorf("PollCount = %d, want 1", stats.PollCount)
	}

	if stats.EventsReceived != 1 {
		t.Errorf("EventsReceived = %d, want 1", stats.EventsReceived)
	}
}
