package notification

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// MQTTClient interface for publishing to MQTT
type MQTTClient interface {
	Publish(topic string, qos byte, retained bool, payload interface{}) error
}

// Publisher publishes notification events to MQTT
type Publisher struct {
	client      MQTTClient
	topicPrefix string
}

// NewPublisher creates a new notification publisher
func NewPublisher(client MQTTClient, topicPrefix string) *Publisher {
	return &Publisher{
		client:      client,
		topicPrefix: topicPrefix,
	}
}

func (p *Publisher) PublishSleepTimelineDate(babyUID, date string) error {
	return p.client.Publish(fmt.Sprintf("%s/babies/%s/sleep_timeline_history_date", p.topicPrefix, babyUID), 0, true, date)
}

// EventPayload is the JSON structure published to event topics
type EventPayload struct {
	ID            int    `json:"id"`
	Type          string `json:"type"`
	BabyUID       string `json:"baby_uid"`
	EventUID      string `json:"event_uid,omitempty"`
	Timestamp     string `json:"timestamp"`
	UnixTimestamp int64  `json:"unix_timestamp"`
}

// PublishEvent publishes an event to the appropriate MQTT topics
func (p *Publisher) PublishEvent(event Event) error {
	log.Debug().
		Str("type", string(event.Type)).
		Str("baby_uid", event.BabyUID).
		Str("state_topic", event.StateTopic()).
		Bool("is_boolean", event.IsBooleanState()).
		Msg("Publishing event to MQTT")

	// Publish to event topic
	if err := p.publishEventTopic(event); err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	// Publish to state topic
	if err := p.publishStateTopic(event); err != nil {
		return fmt.Errorf("failed to publish state: %w", err)
	}

	log.Debug().
		Str("type", string(event.Type)).
		Str("baby_uid", event.BabyUID).
		Msg("Successfully published event to MQTT")

	return nil
}

// publishEventTopic publishes the full event to the event topic
// Topic: {prefix}/babies/{baby_uid}/events/{event_type}
func (p *Publisher) publishEventTopic(event Event) error {
	topic := fmt.Sprintf("%s/babies/%s/events/%s",
		p.topicPrefix, event.BabyUID, event.MQTTTopic())

	payload := EventPayload{
		ID:            event.ID,
		Type:          string(event.Type),
		BabyUID:       event.BabyUID,
		EventUID:      event.EventUID,
		Timestamp:     event.ISOTimestamp(),
		UnixTimestamp: event.UnixTimestamp(),
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	log.Debug().
		Str("topic", topic).
		Str("type", string(event.Type)).
		Msg("Publishing event")

	return p.client.Publish(topic, 0, false, string(jsonPayload))
}

// publishStateTopic publishes the state value to the state topic
// Topic: {prefix}/babies/{baby_uid}/{state_key}
//
// Motion/sound state is owned exclusively by mqtt.Connection.publishEvent
// (WColan-compatible motion/sound + _active topics with 45s generation-guarded
// windows). To keep exactly one writer per motion/sound state topic, this
// publisher emits only the event JSON for MOTION/SOUND and skips the state
// topic here; the notification Manager funnels those events through the
// Connection callback.
func (p *Publisher) publishStateTopic(event Event) error {
	if event.Type == NotificationMotion || event.Type == NotificationSound {
		return nil
	}
	stateTopic := event.StateTopic()
	if stateTopic == "" {
		return nil // No state topic for this event type
	}

	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, event.BabyUID, stateTopic)

	var payload string
	if event.IsBooleanState() {
		// Boolean state (true/false)
		if event.StateValue() == true {
			payload = "true"
		} else {
			payload = "false"
		}
	} else {
		// Timestamp state - use ISO 8601 format for Home Assistant device_class: timestamp
		payload = event.ISOTimestamp()
	}

	log.Info().
		Str("topic", topic).
		Str("payload", payload).
		Str("event_type", string(event.Type)).
		Msg("Publishing state to MQTT")

	// Use retained=true so Home Assistant can read the last value on startup/reconnect
	err := p.client.Publish(topic, 0, true, payload)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("Failed to publish state to MQTT")
	}
	return err
}

type SleepMetadata struct {
	FetchedAt          time.Time
	LastSuccessAt      time.Time
	AvailabilityReason string
}

type SleepProjection struct {
	Asleep             *bool
	InBed              *bool
	Event              *SleepEvent
	State              *SleepState
	FetchedAt          time.Time
	LastSuccessAt      time.Time
	AvailabilityReason string
}

type sleepEventPayload struct {
	EventType  string `json:"event_type"`
	VisitType  string `json:"visit_type,omitempty"`
	OccurredAt string `json:"occurred_at"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

func (p *Publisher) PublishSleepEvent(babyUID string, event SleepEvent) error {
	t, ok := event.Time()
	if !ok {
		return nil
	}
	payload := sleepEventPayload{EventType: event.Key, OccurredAt: t.Format(time.RFC3339Nano)}
	if event.Key == SleepEventKeyVisit && (event.InternalKey == "VISIT" || event.InternalKey == "VISIT_WOKE_UP" || event.InternalKey == "VISIT_FELL_ASLEEP") {
		payload.VisitType = event.InternalKey
	}
	if event.UpdatedAt != nil {
		payload.UpdatedAt = unixRFC3339(event.UpdatedAt)
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(b) > 512 {
		return fmt.Errorf("sleep event payload exceeds 512 bytes")
	}
	return p.client.Publish(fmt.Sprintf("%s/babies/%s/sleep_event", p.topicPrefix, babyUID), 0, false, string(b))
}

func (p *Publisher) PublishLastSleepTimestamps(babyUID string, fellAsleep, wokeUp, parentVisit time.Time) error {
	for _, item := range []struct {
		name  string
		value time.Time
	}{{"last_fell_asleep", fellAsleep}, {"last_wake_up", wokeUp}, {"last_parent_visit", parentVisit}} {
		payload := ""
		if !item.value.IsZero() {
			payload = item.value.UTC().Format(time.RFC3339Nano)
		}
		if err := p.client.Publish(fmt.Sprintf("%s/babies/%s/%s", p.topicPrefix, babyUID, item.name), 0, true, payload); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) PublishSleepTimeline(babyUID string, history *historyFile) error {
	base := fmt.Sprintf("%s/babies/%s", p.topicPrefix, babyUID)
	if history == nil || len(history.Reports) == 0 {
		_ = p.client.Publish(base+"/sleep_timeline", 0, true, "")
		return p.client.Publish(base+"/sleep_timeline/availability", 0, true, "offline")
	}
	r := history.Reports[len(history.Reports)-1]
	rows := make([]map[string]interface{}, 0, 64)
	omitted := 0
	for _, in := range r.Intervals {
		if len(rows) >= 64 {
			omitted++
			continue
		}
		rows = append(rows, map[string]interface{}{"type": "state", "state": in.State, "start": in.Start, "end": in.End, "obsolete": in.Obsolete})
	}
	for _, ev := range history.Events {
		if (r.WindowStart != "" && ev.OccurredAt < r.WindowStart) || (r.WindowEnd != "" && ev.OccurredAt >= r.WindowEnd) {
			continue
		}
		if len(rows) >= 64 {
			omitted++
			continue
		}
		rows = append(rows, map[string]interface{}{"type": "event", "event_type": ev.Key, "visit_type": ev.Subtype, "occurred_at": ev.OccurredAt})
	}
	attrs := map[string]interface{}{"rows": rows, "truncated": omitted > 0, "omitted_rows": omitted, "window_start": r.WindowStart, "window_end": r.WindowEnd, "revision": history.Revision, "history_status": history.HistoryStatus}
	for {
		b, _ := json.Marshal(attrs)
		if len(b) <= 8192 || len(rows) == 0 {
			break
		}
		rows = rows[:len(rows)-1]
		omitted++
		attrs["rows"] = rows
		attrs["truncated"] = true
		attrs["omitted_rows"] = omitted
	}
	if err := p.client.Publish(base+"/sleep_timeline", 0, true, r.Key); err != nil {
		return err
	}
	if err := p.publishJSON(base+"/sleep_timeline_attributes", attrs); err != nil {
		return err
	}
	return p.client.Publish(base+"/sleep_timeline/availability", 0, true, "online")
}

func (p *Publisher) PublishSleepTimelineForDate(babyUID, date string, history *historyFile) error {
	base := fmt.Sprintf("%s/babies/%s", p.topicPrefix, babyUID)
	selected, status := reportForDate(history, date)
	if status != "ok" || selected == nil {
		_ = p.client.Publish(base+"/sleep_timeline_history", 0, true, "")
		return p.client.Publish(base+"/sleep_timeline_history/availability", 0, true, "offline")
	}
	attrs := map[string]interface{}{"rows": []map[string]interface{}{}, "truncated": false, "omitted_rows": 0, "window_start": selected.WindowStart, "window_end": selected.WindowEnd, "revision": selected.Revision, "history_status": history.HistoryStatus, "report_date": date}
	rows := attrs["rows"].([]map[string]interface{})
	for _, in := range selected.Intervals {
		if len(rows) >= 64 {
			attrs["truncated"] = true
			attrs["omitted_rows"] = attrs["omitted_rows"].(int) + 1
			continue
		}
		rows = append(rows, map[string]interface{}{"type": "state", "state": in.State, "start": in.Start, "end": in.End, "obsolete": in.Obsolete})
	}
	for _, ev := range history.Events {
		if (selected.WindowStart != "" && ev.OccurredAt < selected.WindowStart) || (selected.WindowEnd != "" && ev.OccurredAt >= selected.WindowEnd) {
			continue
		}
		if len(rows) >= 64 {
			attrs["truncated"] = true
			attrs["omitted_rows"] = attrs["omitted_rows"].(int) + 1
			continue
		}
		rows = append(rows, map[string]interface{}{"type": "event", "event_type": ev.Key, "visit_type": ev.Subtype, "occurred_at": ev.OccurredAt})
	}
	attrs["rows"] = rows
	for {
		b, _ := json.Marshal(attrs)
		if len(b) <= 8192 || len(rows) == 0 {
			break
		}
		rows = rows[:len(rows)-1]
		attrs["rows"] = rows
		attrs["truncated"] = true
		attrs["omitted_rows"] = attrs["omitted_rows"].(int) + 1
	}
	if err := p.client.Publish(base+"/sleep_timeline_history", 0, true, selected.Key); err != nil {
		return err
	}
	if err := p.publishJSON(base+"/sleep_timeline_history_attributes", attrs); err != nil {
		return err
	}
	return p.client.Publish(base+"/sleep_timeline_history/availability", 0, true, "online")
}

func (p *Publisher) publishJSON(topic string, value interface{}) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.client.Publish(topic, 0, true, string(payload))
}

func (p *Publisher) PublishSleepStats(babyUID string, stats *SleepStats, meta SleepMetadata) error {
	if stats == nil {
		return nil
	}
	base := fmt.Sprintf("%s/babies/%s", p.topicPrefix, babyUID)
	values := []struct {
		name    string
		value   *int
		minutes bool
	}{
		{"times_woke_up", stats.TimesWokeUp, false},
		{"sleep_interventions", stats.SleepInterventions, false},
		{"awake_time_today", stats.TotalAwakeTime, true},
		{"sleep_time_today", stats.TotalSleepTime, true},
	}
	for _, item := range values {
		if item.value == nil {
			continue
		}
		v := *item.value
		if item.minutes {
			v /= 60
		}
		if err := p.client.Publish(base+"/"+item.name, 0, true, fmt.Sprintf("%d", v)); err != nil {
			return err
		}
	}
	publishInt := func(name string, value *int) error {
		payload := ""
		if value != nil && *value >= 0 {
			payload = strconv.Itoa(*value)
		}
		return p.client.Publish(base+"/"+name, 0, true, payload)
	}
	publishNumber := func(name string, value *json.Number, integer bool) error {
		payload := ""
		if value != nil {
			if integer {
				if v, err := value.Int64(); err == nil && v >= 0 {
					payload = strconv.FormatInt(v, 10)
				}
			} else if v, err := value.Float64(); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
				payload = strconv.FormatFloat(v, 'f', -1, 64)
			}
		}
		return p.client.Publish(base+"/"+name, 0, true, payload)
	}
	for _, item := range []struct {
		name  string
		value *int
	}{{"longest_sleep", stats.LongestSleep}, {"times_out_of_crib", stats.TimesOutOfCrib}} {
		if err := publishInt(item.name, item.value); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name    string
		value   *json.Number
		integer bool
	}{{"sleep_onset", stats.SleepOnset, false}, {"total_present_time", stats.TotalPresentTime, false}, {"parent_interventions", stats.ParentInterventions, true}, {"soothing_events", stats.SoothingEvents, true}, {"sleep_sessions", stats.SleepSessions, true}} {
		if err := publishNumber(item.name, item.value, item.integer); err != nil {
			return err
		}
	}
	var inBed *json.Number
	if stats.TimeInBed != nil {
		inBed = stats.TimeInBed.Total
	}
	if err := publishNumber("time_in_bed", inBed, false); err != nil {
		return err
	}
	var score *json.Number
	if stats.SleepScore != nil {
		score = stats.SleepScore.Score
	}
	if score != nil {
		if v, err := score.Int64(); err != nil || v < 0 || v > 100 {
			score = nil
		}
	}
	if err := publishNumber("sleep_score", score, true); err != nil {
		return err
	}
	efficiency := ""
	if stats.SleepQuality != nil && !math.IsNaN(*stats.SleepQuality) && !math.IsInf(*stats.SleepQuality, 0) && *stats.SleepQuality >= 0 && *stats.SleepQuality <= 1 {
		efficiency = strconv.FormatFloat(*stats.SleepQuality*100, 'f', -1, 64)
	}
	if err := p.client.Publish(base+"/sleep_efficiency", 0, true, efficiency); err != nil {
		return err
	}
	for _, item := range []struct {
		name  string
		value *json.Number
	}{{"bed_start_time", stats.BedStartTime}, {"sleep_start_time", stats.SleepStartTime}, {"sleep_end_time", stats.SleepEndTime}, {"last_wake_up", stats.LastWakeUp}} {
		payload := unixNumberRFC3339(item.value)
		if err := p.client.Publish(base+"/"+item.name, 0, true, payload); err != nil {
			return err
		}
	}
	if err := p.client.Publish(base+"/sleep_report_status", 0, true, sleepReportStatus(stats)); err != nil {
		return err
	}
	attrs := map[string]interface{}{"availability_reason": meta.AvailabilityReason}
	if !meta.FetchedAt.IsZero() {
		attrs["fetched_at"] = meta.FetchedAt.UTC().Format(time.RFC3339)
	}
	if !meta.LastSuccessAt.IsZero() {
		attrs["last_success_at"] = meta.LastSuccessAt.UTC().Format(time.RFC3339)
	}
	if stats.Date != nil {
		attrs["date"] = *stats.Date
	}
	if stats.PeriodDate != nil {
		attrs["period_date"] = *stats.PeriodDate
	}
	if stats.PeriodPart != nil {
		attrs["period_part"] = *stats.PeriodPart
	}
	if stats.Kind != nil {
		attrs["kind"] = *stats.Kind
	}
	if stats.Valid != nil {
		attrs["valid"] = *stats.Valid
	}
	if stats.Sealed != nil {
		attrs["sealed"] = *stats.Sealed
	}
	if stats.Ongoing != nil {
		attrs["ongoing"] = *stats.Ongoing
	}
	return p.publishJSON(base+"/sleep_stats_attributes", attrs)
}

func sleepReportStatus(stats *SleepStats) string {
	if stats == nil {
		return "missing"
	}
	if stats.Valid == nil {
		return "unknown"
	}
	if !*stats.Valid {
		return "invalid"
	}
	if stats.Ongoing != nil && *stats.Ongoing {
		return "ongoing"
	}
	if stats.Ongoing != nil && !*stats.Ongoing && stats.Sealed != nil && *stats.Sealed {
		return "completed"
	}
	return "unknown"
}

func unixNumberRFC3339(value *json.Number) string {
	if value == nil {
		return ""
	}
	v, err := value.Float64()
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return ""
	}
	sec, frac := math.Modf(v)
	t := time.Unix(int64(sec), int64(math.Round(frac*float64(time.Second)))).UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func (p *Publisher) PublishSleepProjection(babyUID string, state SleepProjection) error {
	base := fmt.Sprintf("%s/babies/%s", p.topicPrefix, babyUID)
	if state.Asleep != nil {
		if err := p.PublishBoolean(babyUID, "is_asleep", *state.Asleep); err != nil {
			return err
		}
	}
	if state.InBed != nil {
		if err := p.PublishBoolean(babyUID, "in_bed", *state.InBed); err != nil {
			return err
		}
	}
	attrs := map[string]interface{}{"availability_reason": state.AvailabilityReason}
	if state.State != nil {
		attrs["state"] = state.State.Title
		if state.State.BeginTS != nil {
			attrs["begin_ts"] = unixRFC3339(state.State.BeginTS)
		}
		if state.State.EndTS != nil {
			attrs["end_ts"] = unixRFC3339(state.State.EndTS)
		}
	}
	if err := p.publishJSON(base+"/sleep_state_attributes", attrs); err != nil {
		return err
	}
	if state.Event == nil {
		return nil
	}
	if err := p.client.Publish(base+"/last_sleep_event", 0, true, state.Event.Key); err != nil {
		return err
	}
	eventAttrs := map[string]interface{}{}
	if t, ok := state.Event.Time(); ok {
		eventAttrs["event_time"] = t.Format(time.RFC3339)
	}
	if state.Event.Title != nil && safeSleepAttribute(*state.Event.Title) {
		eventAttrs["title"] = *state.Event.Title
	}
	if state.Event.Confidence != nil {
		eventAttrs["confidence_raw"] = *state.Event.Confidence
	}
	if state.Event.Source != nil && safeSleepAttribute(*state.Event.Source) {
		eventAttrs["source"] = *state.Event.Source
	}
	return p.publishJSON(base+"/sleep_event_attributes", eventAttrs)
}

func safeSleepAttribute(value string) bool {
	if value == "" || len(value) > 128 || strings.Contains(value, "://") {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func unixRFC3339(v *float64) string {
	if v == nil || *v <= 0 {
		return ""
	}
	sec, frac := math.Modf(*v)
	return time.Unix(int64(sec), int64(math.Round(frac*float64(time.Second)))).UTC().Format(time.RFC3339Nano)
}

func (p *Publisher) PublishSleepAvailability(babyUID string, stats *SleepStats, statsFresh, event, asleep, inBed bool) {
	base := fmt.Sprintf("%s/babies/%s", p.topicPrefix, babyUID)
	publish := func(object string, available bool) {
		payload := "offline"
		if available {
			payload = "online"
		}
		_ = p.client.Publish(base+"/"+object+"/availability", 0, true, payload)
	}
	publish("times_woke_up", statsFresh && stats != nil && stats.TimesWokeUp != nil)
	publish("sleep_interventions", statsFresh && stats != nil && stats.SleepInterventions != nil)
	publish("awake_time_today", statsFresh && stats != nil && stats.TotalAwakeTime != nil)
	publish("sleep_time_today", statsFresh && stats != nil && stats.TotalSleepTime != nil)
	publish("longest_sleep", statsFresh && stats != nil && stats.LongestSleep != nil)
	publish("sleep_onset", statsFresh && stats != nil && stats.SleepOnset != nil)
	publish("total_present_time", statsFresh && stats != nil && stats.TotalPresentTime != nil)
	publish("time_in_bed", statsFresh && stats != nil && stats.TimeInBed != nil && stats.TimeInBed.Total != nil)
	publish("parent_interventions", statsFresh && stats != nil && stats.ParentInterventions != nil)
	publish("soothing_events", statsFresh && stats != nil && stats.SoothingEvents != nil)
	publish("times_out_of_crib", statsFresh && stats != nil && stats.TimesOutOfCrib != nil)
	publish("sleep_sessions", statsFresh && stats != nil && stats.SleepSessions != nil)
	publish("sleep_score", statsFresh && stats != nil && stats.SleepScore != nil && stats.SleepScore.Score != nil)
	publish("sleep_efficiency", statsFresh && stats != nil && stats.SleepQuality != nil)
	publish("bed_start_time", statsFresh && stats != nil && stats.BedStartTime != nil)
	publish("sleep_start_time", statsFresh && stats != nil && stats.SleepStartTime != nil)
	publish("sleep_end_time", statsFresh && stats != nil && stats.SleepEndTime != nil)
	publish("last_wake_up", statsFresh && stats != nil && stats.LastWakeUp != nil)
	publish("sleep_report_status", statsFresh && stats != nil)
	publish("last_sleep_event", event)
	publish("is_asleep", asleep)
	publish("in_bed", inBed)
}

// Useful for updating motion_timestamp or sound_timestamp
func (p *Publisher) PublishTimestamp(event Event, topicSuffix string) error {
	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, event.BabyUID, topicSuffix)

	// Use ISO 8601 format for Home Assistant device_class: timestamp
	payload := event.ISOTimestamp()

	// Use retained=true so Home Assistant can read the last value
	return p.client.Publish(topic, 0, true, payload)
}

// PublishBoolean publishes a boolean value to a state topic
func (p *Publisher) PublishBoolean(babyUID string, topicSuffix string, value bool) error {
	topic := fmt.Sprintf("%s/babies/%s/%s",
		p.topicPrefix, babyUID, topicSuffix)

	payload := "false"
	if value {
		payload = "true"
	}

	// Use retained=true so Home Assistant can read the last value
	return p.client.Publish(topic, 0, true, payload)
}
