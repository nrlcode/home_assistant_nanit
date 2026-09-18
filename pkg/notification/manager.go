package notification

import (
	"context"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/rs/zerolog/log"
)

// ManagerConfig configures the NotificationManager
type ManagerConfig struct {
	PollerConfig         PollerConfig
	TopicPrefix          string
	Babies               []baby.Baby
	SleepEventInterval   time.Duration
	SleepStatsInterval   time.Duration
	SleepEventStaleAfter time.Duration
	SleepStatsStaleAfter time.Duration
	HistoryDir           string
}

// Manager coordinates polling and publishing of notification events
type Manager struct {
	config        ManagerConfig
	poller        *Poller
	publisher     *Publisher
	fetcher       MessageFetcher
	sleepFetcher  SleepFetcher
	sleepState    map[string]*sleepBabyState
	sleepHistory  *sleepHistory
	selectedDates map[string]string
	OnEvent       func(Event)

	mu          sync.Mutex
	babyBackoff map[string]time.Duration
}

// NewManager creates a new NotificationManager
func NewManager(config ManagerConfig, fetcher MessageFetcher, mqttClient MQTTClient) *Manager {
	if config.SleepEventInterval <= 0 {
		config.SleepEventInterval = defaultSleepEventInterval
	}
	if config.SleepStatsInterval <= 0 {
		config.SleepStatsInterval = defaultSleepStatsInterval
	}
	if config.SleepEventStaleAfter <= 0 {
		config.SleepEventStaleAfter = defaultSleepEventStale
	}
	if config.SleepStatsStaleAfter <= 0 {
		config.SleepStatsStaleAfter = defaultSleepStatsStale
	}
	manager := &Manager{
		config:        config,
		poller:        NewPoller(config.PollerConfig, fetcher),
		publisher:     NewPublisher(mqttClient, config.TopicPrefix),
		fetcher:       fetcher,
		babyBackoff:   make(map[string]time.Duration),
		sleepState:    make(map[string]*sleepBabyState),
		sleepHistory:  newSleepHistory(config.HistoryDir),
		selectedDates: make(map[string]string),
	}
	manager.sleepFetcher, _ = fetcher.(SleepFetcher)
	return manager
}

func (m *Manager) SetSleepTimelineDate(babyUID, date string) {
	if len(date) != len("2006-01-02") {
		_ = m.publisher.PublishSleepTimelineForDate(babyUID, date, nil)
		return
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		_ = m.publisher.PublishSleepTimelineForDate(babyUID, date, nil)
		return
	}
	m.mu.Lock()
	m.selectedDates[babyUID] = date
	m.mu.Unlock()
	if history, err := m.sleepHistory.load(m.historyScopeForBaby(babyUID), time.Now().UTC()); err == nil {
		_ = m.publisher.PublishSleepTimelineForDate(babyUID, date, history)
	} else {
		_ = m.publisher.PublishSleepTimelineForDate(babyUID, date, nil)
	}
}

// Run starts the notification polling loop for all babies
func (m *Manager) Run(ctx context.Context) error {
	if len(m.config.Babies) == 0 {
		log.Warn().Msg("No babies configured for notification polling")
		return nil
	}

	log.Info().
		Int("babies", len(m.config.Babies)).
		Dur("poll_interval", m.config.PollerConfig.PollInterval).
		Float64("jitter", m.config.PollerConfig.Jitter).
		Msg("Starting notification polling")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Notification polling stopped")
			return ctx.Err()
		default:
		}

		for _, babyInfo := range m.config.Babies {
			if err := m.pollBaby(ctx, babyInfo); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			m.pollSleepDue(ctx, babyInfo.UID, time.Now())
		}

		interval := m.poller.NextInterval()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (m *Manager) pollBaby(ctx context.Context, babyInfo baby.Baby) error {
	events, err := m.poller.Poll(ctx, babyInfo.UID)
	if err != nil {
		m.recordBabyFailure(babyInfo.UID)
		log.Error().
			Err(err).
			Str("baby_uid", babyInfo.UID).
			Str("baby_name", babyInfo.Name).
			Int("backoff_errors", m.poller.ErrorCount()).
			Dur("next_backoff", m.poller.CurrentBackoff()).
			Msg("Failed to poll for events")
		return err
	}

	m.recordBabySuccess(babyInfo.UID)

	for _, event := range events {
		log.Info().
			Str("type", string(event.Type)).
			Str("baby_uid", babyInfo.UID).
			Str("baby_name", babyInfo.Name).
			Str("event_uid", event.EventUID).
			Time("timestamp", event.Timestamp).
			Msg("Received notification event")

		if err := m.publisher.PublishEvent(event); err != nil {
			log.Error().
				Err(err).
				Str("type", string(event.Type)).
				Str("baby_uid", babyInfo.UID).
				Msg("Failed to publish event")
		}
		if m.OnEvent != nil {
			m.OnEvent(event)
		}
	}

	return nil
}

// recordBabyFailure advances only the failing baby's backoff; the shared
// poller backoff tracks the max so a later succeeding baby cannot erase it.
func (m *Manager) recordBabyFailure(babyUID string) {
	m.poller.IncrementBackoff()
	m.mu.Lock()
	if m.babyBackoff == nil {
		m.babyBackoff = make(map[string]time.Duration)
	}
	// Independent per-baby exponential: double current or start at base interval.
	cur := m.babyBackoff[babyUID]
	if cur <= 0 {
		cur = m.poller.config.PollInterval
		if cur <= 0 {
			cur = m.config.PollerConfig.PollInterval
		}
	} else {
		cur = time.Duration(float64(cur) * 2)
		if max := m.poller.config.MaxBackoff; max > 0 && cur > max {
			cur = max
		}
		if max := m.config.PollerConfig.MaxBackoff; max > 0 && cur > max {
			cur = max
		}
	}
	// Keep per-baby exponential independent.
	m.babyBackoff[babyUID] = cur
	maxBackoff := cur
	for _, b := range m.babyBackoff {
		if b > maxBackoff {
			maxBackoff = b
		}
	}
	m.mu.Unlock()
	m.poller.mu.Lock()
	m.poller.backoff = maxBackoff
	m.poller.mu.Unlock()
}

// recordBabySuccess resets only the succeeding baby; global backoff stays at
// the max of remaining failing babies.
func (m *Manager) recordBabySuccess(babyUID string) {
	m.mu.Lock()
	if m.babyBackoff != nil {
		delete(m.babyBackoff, babyUID)
	}
	maxBackoff := time.Duration(0)
	for _, b := range m.babyBackoff {
		if b > maxBackoff {
			maxBackoff = b
		}
	}
	m.mu.Unlock()
	m.poller.mu.Lock()
	if maxBackoff == 0 {
		m.poller.errorCount = 0
		m.poller.backoff = 0
	} else {
		m.poller.backoff = maxBackoff
	}
	m.poller.mu.Unlock()
}

// Stats returns the current poller statistics
func (m *Manager) Stats() PollerStats {
	return m.poller.Stats()
}
