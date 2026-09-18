package notification

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/message"
)

type sleepMQTTAttempt struct {
	topic    string
	retained bool
	payload  string
}

type sleepMQTTRecorder struct {
	mu             sync.Mutex
	attempts       []sleepMQTTAttempt
	failSleepEvent bool
}

func (r *sleepMQTTRecorder) Publish(topic string, _ byte, retained bool, payload interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, _ := json.Marshal(payload)
	if s, ok := payload.(string); ok {
		b = []byte(s)
	}
	r.attempts = append(r.attempts, sleepMQTTAttempt{topic: topic, retained: retained, payload: string(b)})
	if r.failSleepEvent && strings.HasSuffix(topic, "/sleep_event") {
		return errors.New("synthetic publish failure")
	}
	return nil
}

func (r *sleepMQTTRecorder) sleepEvents() []sleepMQTTAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []sleepMQTTAttempt
	for _, attempt := range r.attempts {
		if strings.HasSuffix(attempt.topic, "/sleep_event") {
			out = append(out, attempt)
		}
	}
	return out
}

type sleepManagerFetcher struct {
	stats  map[string]SleepStatsResponse
	events map[string][]SleepEvent
	err    error
}

func (f *sleepManagerFetcher) FetchMessages(string, int) ([]message.Message, error)     { return nil, nil }
func (f *sleepManagerFetcher) FetchNewMessages(string, time.Duration) []message.Message { return nil }
func (f *sleepManagerFetcher) TryFetchSleepStatsCtx(ctx context.Context, uid string) (SleepStatsResponse, error) {
	if f.err != nil {
		return SleepStatsResponse{}, f.err
	}
	return f.stats[uid], nil
}
func (f *sleepManagerFetcher) TryFetchSleepEventsCtx(ctx context.Context, uid string, limit int) ([]SleepEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.events[uid], nil
}
func (f *sleepManagerFetcher) TryFetchLastSleepEventCtx(ctx context.Context, uid string) (*SleepEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	return nil, nil
}

func TestSleepManagerIsolatesBabiesAndStales(t *testing.T) {
	valid := true
	zero := 0
	one := 1
	asleep := float64(1700000000)
	awake := float64(1700000100)
	f := &sleepManagerFetcher{stats: map[string]SleepStatsResponse{
		"a": {Exists: true, Latest: &SleepStats{Valid: &valid, TimesWokeUp: &zero, States: []SleepState{{Title: SleepStateAsleep, BeginTS: &asleep}}}},
		"b": {Exists: true, Latest: &SleepStats{Valid: &valid, TimesWokeUp: &one, States: []SleepState{{Title: SleepStateAwake, BeginTS: &awake}}}},
	}, events: map[string][]SleepEvent{}}
	mqtt := NewMockMQTTClient()
	m := NewManager(ManagerConfig{PollerConfig: DefaultPollerConfig(), TopicPrefix: "nanit", SleepStatsInterval: time.Minute, SleepEventInterval: time.Minute, SleepStatsStaleAfter: time.Minute, SleepEventStaleAfter: time.Minute}, f, mqtt)
	now := time.Now()
	m.pollSleepDue(context.Background(), "a", now)
	m.pollSleepDue(context.Background(), "b", now)
	if v, _ := mqtt.GetPublished("nanit/babies/a/times_woke_up"); v != "0" {
		t.Fatalf("a=%#v", v)
	}
	if v, _ := mqtt.GetPublished("nanit/babies/b/times_woke_up"); v != "1" {
		t.Fatalf("b=%#v", v)
	}
	m.publishSleepState("a", now.Add(2*time.Minute), m.sleepState["a"])
	if v, _ := mqtt.GetPublished("nanit/babies/a/times_woke_up/availability"); v != "offline" {
		t.Fatalf("stale=%#v", v)
	}
}

func TestSleepRetryIsIndependentAndCapped(t *testing.T) {
	f := &sleepManagerFetcher{err: errors.New("synthetic")}
	m := NewManager(ManagerConfig{PollerConfig: PollerConfig{PollInterval: time.Second, MaxBackoff: 4 * time.Second}, TopicPrefix: "nanit", SleepStatsInterval: time.Second, SleepEventInterval: time.Second}, f, NewMockMQTTClient())
	now := time.Now()
	m.pollSleepDue(context.Background(), "a", now)
	m.pollSleepDue(context.Background(), "a", now.Add(time.Second))
	m.pollSleepDue(context.Background(), "a", now.Add(3*time.Second))
	if d := m.sleepState["a"].nextStats.Sub(now.Add(3 * time.Second)); d > 4*time.Second {
		t.Fatalf("retry=%v", d)
	}
	if _, ok := m.babyBackoff["a"]; ok {
		t.Fatal("sleep failure contaminated message backoff")
	}
}

func TestSleepEventReplaySuppression(t *testing.T) {
	now := time.Unix(1789746000, 0).UTC()
	event := func(uid, key string, at time.Time) SleepEvent {
		raw := float64(at.UnixNano()) / float64(time.Second)
		return SleepEvent{UID: uid, Key: key, TimeRaw: &raw}
	}
	f := &sleepManagerFetcher{stats: map[string]SleepStatsResponse{}, events: map[string][]SleepEvent{
		"baby": {event("seed", SleepEventKeyFellAsleep, now.Add(-20*time.Second))},
	}}
	recorder := &sleepMQTTRecorder{}
	config := ManagerConfig{PollerConfig: DefaultPollerConfig(), TopicPrefix: "nanit", SleepEventInterval: time.Second, SleepEventStaleAfter: time.Minute}
	m := NewManager(config, f, recorder)
	state := &sleepBabyState{}

	m.pollSleepEvents(context.Background(), "baby", now, state)
	m.pollSleepEvents(context.Background(), "baby", now.Add(time.Second), state)
	if got := len(recorder.sleepEvents()); got != 0 {
		t.Fatalf("seed/repeated batch published %d events", got)
	}

	tie := now.Add(-10 * time.Second)
	f.events["baby"] = []SleepEvent{
		event("seed", SleepEventKeyFellAsleep, now.Add(-20*time.Second)),
		event("tie-b", SleepEventKeyWokeUp, tie),
		event("tie-a", SleepEventKeyVisit, tie),
	}
	m.pollSleepEvents(context.Background(), "baby", now.Add(2*time.Second), state)
	events := recorder.sleepEvents()
	if len(events) != 2 {
		t.Fatalf("fresh tie attempts=%d, want 2", len(events))
	}
	if !strings.Contains(events[0].payload, SleepEventKeyVisit) || !strings.Contains(events[1].payload, SleepEventKeyWokeUp) {
		t.Fatalf("tie order=%#v", events)
	}
	for _, attempt := range events {
		if attempt.retained {
			t.Fatal("sleep event must be non-retained")
		}
	}

	f.events["baby"] = append(f.events["baby"], event("older-backfill", SleepEventKeyRemoved, now.Add(-30*time.Second)))
	f.events["baby"][1].Key = SleepEventKeyRemoved
	m.pollSleepEvents(context.Background(), "baby", now.Add(3*time.Second), state)
	if got := len(recorder.sleepEvents()); got != 2 {
		t.Fatalf("replay/backfill/correction attempts=%d", got)
	}

	restartedRecorder := &sleepMQTTRecorder{}
	restarted := NewManager(config, f, restartedRecorder)
	restarted.pollSleepEvents(context.Background(), "baby", now.Add(4*time.Second), &sleepBabyState{})
	if got := len(restartedRecorder.sleepEvents()); got != 0 {
		t.Fatalf("restart published %d events", got)
	}
}

func TestSleepEventPublishFailureIsAtMostOnceAndSnapshotsRecover(t *testing.T) {
	now := time.Unix(1789746000, 0).UTC()
	seedTime := float64(now.Add(-20 * time.Second).Unix())
	freshTime := float64(now.Add(-5 * time.Second).Unix())
	f := &sleepManagerFetcher{stats: map[string]SleepStatsResponse{}, events: map[string][]SleepEvent{
		"baby": {{UID: "seed", Key: SleepEventKeyFellAsleep, TimeRaw: &seedTime}},
	}}
	recorder := &sleepMQTTRecorder{}
	m := NewManager(ManagerConfig{PollerConfig: DefaultPollerConfig(), TopicPrefix: "nanit", SleepEventInterval: time.Second, SleepEventStaleAfter: time.Minute}, f, recorder)
	state := &sleepBabyState{}
	m.pollSleepEvents(context.Background(), "baby", now, state)

	recorder.failSleepEvent = true
	f.events["baby"] = append(f.events["baby"], SleepEvent{UID: "fresh", Key: SleepEventKeyWokeUp, TimeRaw: &freshTime})
	m.pollSleepEvents(context.Background(), "baby", now.Add(time.Second), state)
	if got := len(recorder.sleepEvents()); got != 1 {
		t.Fatalf("failed publish attempts=%d, want 1", got)
	}
	recorder.failSleepEvent = false
	m.pollSleepEvents(context.Background(), "baby", now.Add(2*time.Second), state)
	if got := len(recorder.sleepEvents()); got != 1 {
		t.Fatalf("failed event retried: attempts=%d", got)
	}

	seenRetainedSnapshot := false
	for _, attempt := range recorder.attempts {
		if attempt.retained && (strings.HasSuffix(attempt.topic, "/last_wake_up") || strings.HasSuffix(attempt.topic, "/sleep_timeline/availability")) {
			seenRetainedSnapshot = true
		}
	}
	if !seenRetainedSnapshot {
		t.Fatal("retained snapshots were not published after event failure")
	}
}

func TestSleepMQTTPayloadPrivacyAllowlist(t *testing.T) {
	now := time.Unix(1789746000, 0).UTC()
	canary := "PRIVATE-CANARY-DO-NOT-PUBLISH"
	when := float64(now.Add(-10 * time.Second).Unix())
	title, source := canary, "https://signed.invalid/"+canary
	f := &sleepManagerFetcher{stats: map[string]SleepStatsResponse{}, events: map[string][]SleepEvent{
		"baby": {{UID: canary, BabyUID: "baby", Key: SleepEventKeyVisit, InternalKey: "VISIT", Title: &title, Source: &source, TimeRaw: &when}},
	}}
	recorder := &sleepMQTTRecorder{}
	m := NewManager(ManagerConfig{PollerConfig: DefaultPollerConfig(), TopicPrefix: "nanit", SleepEventInterval: time.Second, SleepEventStaleAfter: time.Minute}, f, recorder)
	state := &sleepBabyState{}
	m.pollSleepEvents(context.Background(), "baby", now, state)
	fresh := float64(now.Add(-5 * time.Second).Unix())
	f.events["baby"] = append(f.events["baby"], SleepEvent{UID: canary + "-2", BabyUID: "baby", Key: SleepEventKeyWokeUp, Title: &title, Source: &source, TimeRaw: &fresh})
	m.pollSleepEvents(context.Background(), "baby", now.Add(time.Second), state)
	for _, attempt := range recorder.attempts {
		if strings.Contains(attempt.payload, canary) || strings.Contains(attempt.payload, "https://") {
			t.Fatalf("privacy canary leaked on %s: %s", attempt.topic, attempt.payload)
		}
	}
	if got := len(recorder.sleepEvents()); got != 1 {
		t.Fatalf("sleep event attempts=%d", got)
	}
}
