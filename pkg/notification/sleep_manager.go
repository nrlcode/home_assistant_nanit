package notification

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultSleepEventInterval = 30 * time.Second
	defaultSleepStatsInterval = 5 * time.Minute
	defaultSleepEventStale    = 90 * time.Second
	defaultSleepStatsStale    = 15 * time.Minute
)

type SleepFetcher interface {
	TryFetchSleepStatsCtx(context.Context, string) (SleepStatsResponse, error)
	TryFetchSleepEventsCtx(context.Context, string, int) ([]SleepEvent, error)
	TryFetchLastSleepEventCtx(context.Context, string) (*SleepEvent, error)
}

type sleepBabyState struct {
	nextStats, nextEvents      time.Time
	statsSuccess, eventSuccess time.Time
	stats                      *SleepStats
	event                      *SleepEvent
	statsFailure, eventFailure int
	eventsSeeded               bool
	watermark                  time.Time
	watermarkKeys              map[string]struct{}
	lastFellAsleep             time.Time
	lastWokeUp                 time.Time
	lastParentVisit            time.Time
}

func (m *Manager) pollSleepDue(ctx context.Context, babyUID string, now time.Time) {
	if m.sleepFetcher == nil {
		return
	}
	state := m.sleepState[babyUID]
	if state == nil {
		state = &sleepBabyState{}
		m.sleepState[babyUID] = state
	}
	if state.nextStats.IsZero() || !now.Before(state.nextStats) {
		m.pollSleepStats(ctx, babyUID, now, state)
	}
	if state.nextEvents.IsZero() || !now.Before(state.nextEvents) {
		m.pollSleepEvents(ctx, babyUID, now, state)
	}
	m.publishSleepState(babyUID, now, state)
}

func (m *Manager) publishSelectedSleepTimeline(babyUID string, history *historyFile) {
	m.mu.Lock()
	date := m.selectedDates[babyUID]
	m.mu.Unlock()
	if date != "" {
		_ = m.publisher.PublishSleepTimelineForDate(babyUID, date, history)
	}
}

func (m *Manager) pollSleepStats(ctx context.Context, babyUID string, now time.Time, state *sleepBabyState) {
	response, err := m.sleepFetcher.TryFetchSleepStatsCtx(ctx, babyUID)
	if err != nil {
		state.statsFailure++
		state.nextStats = now.Add(sleepRetry(m.config.SleepStatsInterval, m.config.PollerConfig.MaxBackoff, state.statsFailure))
		return
	}
	state.statsFailure = 0
	state.nextStats = now.Add(m.config.SleepStatsInterval)
	if !response.Exists || response.Latest == nil || response.Latest.Valid == nil || !*response.Latest.Valid {
		state.stats = nil
		return
	}
	state.stats, state.statsSuccess = response.Latest, now
	_ = m.publisher.PublishSleepStats(babyUID, state.stats, SleepMetadata{FetchedAt: now, LastSuccessAt: now, AvailabilityReason: "ok"})
	if history, err := m.sleepHistory.updateReport(m.historyScopeForBaby(babyUID), state.stats, now); err != nil {
		log.Warn().Err(err).Msg("sleep history report update failed")
	} else {
		_ = m.publisher.PublishSleepTimeline(babyUID, history)
		m.publishSelectedSleepTimeline(babyUID, history)
	}
}

func (m *Manager) pollSleepEvents(ctx context.Context, babyUID string, now time.Time, state *sleepBabyState) {
	events, err := m.sleepFetcher.TryFetchSleepEventsCtx(ctx, babyUID, 200)
	if err != nil {
		state.eventFailure++
		state.nextEvents = now.Add(sleepRetry(m.config.SleepEventInterval, m.config.PollerConfig.MaxBackoff, state.eventFailure))
		return
	}
	last, err := m.sleepFetcher.TryFetchLastSleepEventCtx(ctx, babyUID)
	if err != nil {
		state.eventFailure++
		state.nextEvents = now.Add(sleepRetry(m.config.SleepEventInterval, m.config.PollerConfig.MaxBackoff, state.eventFailure))
		return
	}
	state.eventFailure = 0
	state.nextEvents = now.Add(m.config.SleepEventInterval)
	if last != nil {
		events = append(events, *last)
	}
	sort.SliceStable(events, func(i, j int) bool {
		ti, iok := events[i].Time()
		tj, jok := events[j].Time()
		if !iok {
			return false
		}
		if !jok {
			return true
		}
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if events[i].InternalKey != events[j].InternalKey {
			return events[i].InternalKey < events[j].InternalKey
		}
		return sleepEventPrivateKey(events[i]) < sleepEventPrivateKey(events[j])
	})
	var selected *SleepEvent
	for i := range events {
		e := &events[i]
		if e.BabyUID != "" && e.BabyUID != babyUID {
			continue
		}
		if e.CameraUID != "" && !m.cameraMatches(babyUID, e.CameraUID) {
			continue
		}
		t, ok := e.Time()
		if !ok {
			continue
		}
		if newerSleepEvent(e, selected) {
			selected = e
		}
		switch e.Key {
		case SleepEventKeyFellAsleep:
			if t.After(state.lastFellAsleep) {
				state.lastFellAsleep = t
			}
		case SleepEventKeyWokeUp:
			if t.After(state.lastWokeUp) {
				state.lastWokeUp = t
			}
		case SleepEventKeyVisit:
			if t.After(state.lastParentVisit) {
				state.lastParentVisit = t
			}
		}
		key := sleepEventPrivateKey(*e)
		_, tiedSeen := state.watermarkKeys[key]
		fresh := now.Sub(t) >= 0 && now.Sub(t) <= m.config.SleepEventStaleAfter
		if state.eventsSeeded && supportedSleepEvent(*e) && fresh && (t.After(state.watermark) || (t.Equal(state.watermark) && !tiedSeen)) {
			if err := m.publisher.PublishSleepEvent(babyUID, *e); err != nil {
				log.Warn().Err(err).Msg("sleep event publish failed")
			}
		}
		if t.After(state.watermark) {
			state.watermark = t
			state.watermarkKeys = map[string]struct{}{key: {}}
		} else if t.Equal(state.watermark) {
			if state.watermarkKeys == nil {
				state.watermarkKeys = map[string]struct{}{}
			}
			state.watermarkKeys[key] = struct{}{}
		}
	}
	state.eventsSeeded = true
	state.event, state.eventSuccess = selected, now
	if history, err := m.sleepHistory.updateEvents(m.historyScopeForBaby(babyUID), events, state.watermark, state.watermarkKeys, now); err != nil {
		log.Warn().Err(err).Msg("sleep history event update failed")
	} else {
		_ = m.publisher.PublishSleepTimeline(babyUID, history)
		m.publishSelectedSleepTimeline(babyUID, history)
	}
	if err := m.publisher.PublishLastSleepTimestamps(babyUID, state.lastFellAsleep, state.lastWokeUp, state.lastParentVisit); err != nil {
		log.Warn().Err(err).Msg("sleep timestamp publish failed")
	}
}

func sleepEventPrivateKey(e SleepEvent) string {
	if e.UID != "" {
		return e.UID
	}
	t, _ := e.Time()
	return fmt.Sprintf("%x", sha256.Sum256([]byte(e.CameraUID+"\x00"+e.Key+"\x00"+e.InternalKey+"\x00"+t.Format(time.RFC3339Nano))))
}

func supportedSleepEvent(e SleepEvent) bool {
	switch e.Key {
	case SleepEventKeyFellAsleep, SleepEventKeyWokeUp, SleepEventKeyPutInBed, SleepEventKeyPutToSleep, SleepEventKeyRemoved, SleepEventKeyRemovedAsleep, SleepEventKeyVisit:
		return true
	}
	return false
}

func (m *Manager) cameraMatches(babyUID, cameraUID string) bool {
	for _, b := range m.config.Babies {
		if b.UID == babyUID {
			return b.CameraUID == "" || b.CameraUID == cameraUID
		}
	}
	return true
}

func (m *Manager) historyScopeForBaby(babyUID string) string {
	for _, b := range m.config.Babies {
		if b.UID == babyUID {
			return historyScope(babyUID, b.CameraUID)
		}
	}
	return historyScope(babyUID, "")
}

func newerSleepEvent(candidate, current *SleepEvent) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	ct, cok := candidate.Time()
	rt, rok := current.Time()
	if !cok {
		return false
	}
	if !rok || ct.After(rt) {
		return true
	}
	return ct.Equal(rt) && candidate.UID > current.UID
}

func (m *Manager) publishSleepState(babyUID string, now time.Time, state *sleepBabyState) {
	projection := SleepProjection{AvailabilityReason: "missing"}
	statsFresh := state.stats != nil && now.Sub(state.statsSuccess) <= m.config.SleepStatsStaleAfter
	eventFresh := false
	if state.event != nil {
		if occurred, ok := state.event.Time(); ok {
			age := now.Sub(occurred)
			eventFresh = age >= 0 && age <= m.config.SleepEventStaleAfter && now.Sub(state.eventSuccess) <= m.config.SleepEventStaleAfter
		}
	}
	if statsFresh && state.stats.Ongoing != nil && *state.stats.Ongoing {
		projection.State = currentSleepState(state.stats)
		if projection.State != nil && projection.State.EndTS == nil && (projection.State.Obsolete == nil || !*projection.State.Obsolete) && sleepStateOrder(*projection.State) > 0 && sleepStateOrder(*projection.State) <= float64(now.UnixNano())/1e9 {
			a, b, ok := booleansForSleepState(projection.State.Title)
			if ok {
				projection.Asleep, projection.InBed = &a, &b
				projection.AvailabilityReason = "ok"
			}
		}
	}
	if eventFresh {
		projection.Event = state.event
		eventTime, eventOK := state.event.Time()
		stateOrder := float64(0)
		if projection.State != nil {
			stateOrder = sleepStateOrder(*projection.State)
		}
		if eventOK && float64(eventTime.Unix()) >= stateOrder {
			applySleepEvent(&projection, state.event)
		}
		projection.AvailabilityReason = "ok"
	}
	_ = m.publisher.PublishSleepProjection(babyUID, projection)
	m.publisher.PublishSleepAvailability(babyUID, state.stats, statsFresh, eventFresh, projection.Asleep != nil, projection.InBed != nil)
}

func booleansForSleepState(title string) (bool, bool, bool) {
	switch title {
	case SleepStateAsleep:
		return true, true, true
	case SleepStateAwake, SleepStateParentIntervention:
		return false, true, true
	case SleepStateAbsent:
		return false, false, true
	default:
		return false, false, false
	}
}

func applySleepEvent(p *SleepProjection, event *SleepEvent) {
	if event == nil {
		return
	}
	switch event.Key {
	case SleepEventKeyFellAsleep:
		v := true
		p.Asleep = &v
	case SleepEventKeyWokeUp:
		v := false
		p.Asleep = &v
	case SleepEventKeyPutInBed, SleepEventKeyPutToSleep:
		v := true
		p.InBed = &v
	case SleepEventKeyRemoved, SleepEventKeyRemovedAsleep:
		v := false
		p.InBed = &v
	}
}

func sleepRetry(base, max time.Duration, failures int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	d := base
	for i := 1; i < failures && d < max; i++ {
		d *= 2
	}
	if max > 0 && d > max {
		return max
	}
	return d
}
