package notification

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

// MessageFetcher interface for fetching messages from the API
type MessageFetcher interface {
	FetchMessages(babyUID string, limit int) ([]message.Message, error)
	FetchNewMessages(babyUID string, timeout time.Duration) []message.Message
}

// ContextMessageFetcher is implemented by fetchers that propagate HTTP
// errors and request cancellation. Poller prefers it when available so
// operational backoff reflects real fetch failures.
type ContextMessageFetcher interface {
	FetchNewMessagesCtx(ctx context.Context, babyUID string, timeout time.Duration) ([]message.Message, error)
}

// PollerConfig configures the notification poller
type PollerConfig struct {
	// PollInterval is the base interval between polls (default 10s)
	PollInterval time.Duration

	// Jitter is the jitter factor 0-1, e.g. 0.3 = ±30% (default 0.3)
	Jitter float64

	// MaxBackoff is the maximum backoff duration (default 5m)
	MaxBackoff time.Duration

	// MessageLimit is the number of messages to fetch per poll (default 50)
	MessageLimit int

	// MessageTimeout is how old messages can be to still be processed (default 5m)
	MessageTimeout time.Duration

	// DedupMaxSize is the maximum number of message IDs to track for deduplication (default 1000)
	DedupMaxSize int
}

// DefaultPollerConfig returns the default poller configuration
func DefaultPollerConfig() PollerConfig {
	return PollerConfig{
		PollInterval:   10 * time.Second,
		Jitter:         0.3,
		MaxBackoff:     5 * time.Minute,
		MessageLimit:   50,
		MessageTimeout: 5 * time.Minute,
		DedupMaxSize:   1000,
	}
}

// PollerStats contains statistics about poller operation
type PollerStats struct {
	PollCount      int64
	EventsReceived int64
	ErrorCount     int64
	StartedAt      time.Time
}

// Poller polls for notification events from the Nanit API
type Poller struct {
	config  PollerConfig
	fetcher MessageFetcher
	dedup   *Deduplicator

	mu         sync.RWMutex
	errorCount int
	backoff    time.Duration
	stats      PollerStats
	lastPollAt time.Time
}

// NewPoller creates a new notification poller
func NewPoller(config PollerConfig, fetcher MessageFetcher) *Poller {
	return &Poller{
		config:  config,
		fetcher: fetcher,
		dedup:   NewDeduplicator(config.DedupMaxSize),
		stats: PollerStats{
			StartedAt: time.Now(),
		},
	}
}

// Poll fetches new events for a baby, applying deduplication
func (p *Poller) Poll(ctx context.Context, babyUID string) ([]Event, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	p.mu.Lock()
	p.stats.PollCount++
	p.lastPollAt = time.Now()
	p.mu.Unlock()

	// Fetch messages, preferring the context-aware path so HTTP failures
	// and cancellation propagate to backoff instead of reading as success.
	var messages []message.Message
	if cf, ok := p.fetcher.(ContextMessageFetcher); ok {
		var err error
		messages, err = cf.FetchNewMessagesCtx(ctx, babyUID, p.config.MessageTimeout)
		if err != nil {
			return nil, err
		}
	} else {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		messages = p.fetcher.FetchNewMessages(babyUID, p.config.MessageTimeout)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Filter and parse
	var events []Event
	for _, msg := range messages {
		keyBaby := babyUID
		if msg.BabyUid != "" {
			keyBaby = msg.BabyUid
		}
		// Skip already-seen messages
		if p.dedup.IsSeen(keyBaby, msg.Id) {
			continue
		}

		// Parse the message
		event, err := ParseMessage(msg)
		if err != nil {
			continue
		}

		// Mark as seen
		p.dedup.MarkSeen(keyBaby, msg.Id)
		events = append(events, *event)
	}

	// Update stats
	p.mu.Lock()
	p.stats.EventsReceived += int64(len(events))
	p.mu.Unlock()

	return events, nil
}

// NextInterval returns the next poll interval with jitter applied
func (p *Poller) NextInterval() time.Duration {
	p.mu.RLock()
	backoff := p.backoff
	p.mu.RUnlock()

	base := p.config.PollInterval + backoff
	return p.applyJitter(base)
}

// applyJitter applies jitter to a duration
func (p *Poller) applyJitter(d time.Duration) time.Duration {
	if p.config.Jitter <= 0 {
		return d
	}

	// Random factor between -jitter and +jitter
	jitterFactor := (rand.Float64()*2 - 1) * p.config.Jitter
	multiplier := 1.0 + jitterFactor

	return time.Duration(float64(d) * multiplier)
}

// CurrentBackoff returns the current backoff duration
func (p *Poller) CurrentBackoff() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.backoff
}

// IncrementBackoff increases the backoff after an error
func (p *Poller) IncrementBackoff() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCount++
	p.stats.ErrorCount++

	// Exponential backoff: 2^errorCount * PollInterval
	backoff := time.Duration(math.Pow(2, float64(p.errorCount))) * p.config.PollInterval

	// Cap at MaxBackoff
	if backoff > p.config.MaxBackoff {
		backoff = p.config.MaxBackoff
	}

	p.backoff = backoff
}

// ResetBackoff resets the backoff after a successful poll
func (p *Poller) ResetBackoff() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCount = 0
	p.backoff = 0
}

// ErrorCount returns the current consecutive error count
func (p *Poller) ErrorCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.errorCount
}

// Stats returns current poller statistics
func (p *Poller) Stats() PollerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

// LastPollAt returns when the last poll occurred
func (p *Poller) LastPollAt() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastPollAt
}
