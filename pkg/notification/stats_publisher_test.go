package notification

import (
	"encoding/json"
	"strings"
	"testing"
)

func intp(v int) *int           { return &v }
func boolp(v bool) *bool        { return &v }
func floatp(v float64) *float64 { return &v }

type sleepCapture struct{ values map[string]interface{} }

func (c *sleepCapture) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	if c.values == nil {
		c.values = map[string]interface{}{}
	}
	c.values[topic] = payload
	return nil
}

func TestExpandedSleepReportPublication(t *testing.T) {
	c := &sleepCapture{}
	p := NewPublisher(c, "nanit")
	one := json.Number("1")
	quality := 0.75
	stats := &SleepStats{Valid: boolp(true), TimesWokeUp: intp(0), SleepInterventions: intp(2), TotalAwakeTime: intp(125), TotalSleepTime: intp(3601), LongestSleep: intp(999), SleepSessions: &one, SleepQuality: &quality}
	if err := p.PublishSleepStats("baby", stats, SleepMetadata{AvailabilityReason: "ok"}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"times_woke_up": "0", "sleep_interventions": "2", "awake_time_today": "2", "sleep_time_today": "60", "longest_sleep": "999", "sleep_sessions": "1", "sleep_efficiency": "75"}
	for object, value := range want {
		if got := c.values["nanit/babies/baby/"+object]; got != value {
			t.Errorf("%s = %#v", object, got)
		}
	}
	if got := c.values["nanit/babies/baby/parent_interventions"]; got != "" {
		t.Errorf("missing value = %#v, want tombstone", got)
	}
	if got := c.values["nanit/babies/baby/sleep_stats_attributes"]; got == nil {
		t.Error("missing attributes")
	}
}

func TestSleepReportStatus(t *testing.T) {
	c := &sleepCapture{}
	p := NewPublisher(c, "nanit")
	sealed, ongoing, valid := true, false, true
	if err := p.PublishSleepStats("baby", &SleepStats{Valid: &valid, Sealed: &sealed, Ongoing: &ongoing}, SleepMetadata{AvailabilityReason: "ok"}); err != nil {
		t.Fatal(err)
	}
	if got := c.values["nanit/babies/baby/sleep_report_status"]; got != "completed" {
		t.Fatalf("status=%#v", got)
	}
}

func TestPublishSleepStatsMissingDoesNotManufactureZero(t *testing.T) {
	c := &sleepCapture{}
	p := NewPublisher(c, "nanit")
	stats := &SleepStats{Valid: boolp(true), TimesWokeUp: intp(0)}
	if err := p.PublishSleepStats("baby", stats, SleepMetadata{AvailabilityReason: "missing"}); err != nil {
		t.Fatal(err)
	}
	if got := c.values["nanit/babies/baby/times_woke_up"]; got != "0" {
		t.Fatalf("present zero = %#v", got)
	}
	if _, ok := c.values["nanit/babies/baby/sleep_interventions"]; ok {
		t.Error("missing value published")
	}
}

func TestPublishSleepStateAndEventAttributes(t *testing.T) {
	c := &sleepCapture{}
	p := NewPublisher(c, "nanit")
	ts := float64(1700000000)
	e := &SleepEvent{Key: SleepEventKeyFellAsleep, BabyUID: "baby", CameraUID: "camera", UID: "e1", TimeRaw: &ts}
	if err := p.PublishSleepProjection("baby", SleepProjection{Asleep: boolp(true), InBed: boolp(true), Event: e, AvailabilityReason: "ok"}); err != nil {
		t.Fatal(err)
	}
	if c.values["nanit/babies/baby/is_asleep"] != "true" || c.values["nanit/babies/baby/in_bed"] != "true" {
		t.Fatalf("state %#v", c.values)
	}
	if c.values["nanit/babies/baby/last_sleep_event"] != SleepEventKeyFellAsleep {
		t.Fatalf("event %#v", c.values)
	}
	var attrs map[string]interface{}
	if err := json.Unmarshal([]byte(c.values["nanit/babies/baby/sleep_event_attributes"].(string)), &attrs); err != nil {
		t.Fatal(err)
	}
	if attrs["event_time"] != "2023-11-14T22:13:20Z" {
		t.Fatalf("attrs %#v", attrs)
	}
}

func TestSleepReportMQTTPayloadPrivacyAllowlist(t *testing.T) {
	const canary = "PRIVATE-REPORT-CANARY"
	raw := `{
		"valid":true,
		"period_date":"2026-09-18",
		"media_urls":["https://signed.invalid/PRIVATE-REPORT-CANARY"],
		"read_by":["PRIVATE-REPORT-CANARY"],
		"seen_by":{"id":"PRIVATE-REPORT-CANARY"},
		"shown_by":"PRIVATE-REPORT-CANARY",
		"corrections":{"note":"PRIVATE-REPORT-CANARY"},
		"reports":[{"private":"PRIVATE-REPORT-CANARY"}],
		"sleep_sessions_details":{"private":"PRIVATE-REPORT-CANARY"},
		"longest_sleep_details":{"private":"PRIVATE-REPORT-CANARY"},
		"unknown_nested":{"private":"PRIVATE-REPORT-CANARY"},
		"states":[{"title":"ASLEEP","begin_ts":1789745000,"uid":"PRIVATE-REPORT-CANARY","baby_uid":"PRIVATE-REPORT-CANARY"}]
	}`
	var stats SleepStats
	if err := json.Unmarshal([]byte(raw), &stats); err != nil {
		t.Fatal(err)
	}
	c := &sleepCapture{}
	if err := NewPublisher(c, "nanit").PublishSleepStats("baby", &stats, SleepMetadata{AvailabilityReason: "ok"}); err != nil {
		t.Fatal(err)
	}
	for topic, payload := range c.values {
		var text string
		switch value := payload.(type) {
		case string:
			text = value
		default:
			b, _ := json.Marshal(value)
			text = string(b)
		}
		if strings.Contains(text, canary) || strings.Contains(text, "https://") {
			t.Fatalf("privacy canary leaked on %s: %s", topic, text)
		}
	}
}
