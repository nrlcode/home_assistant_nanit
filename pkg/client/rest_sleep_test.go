package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
)

type sleepRoundTripper func(*http.Request) (*http.Response, error)

func (f sleepRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestSleepEndpointsPreservePresenceAndPaths(t *testing.T) {
	old := myClient
	defer func() { myClient = old }()
	seen := map[string]bool{}
	myClient = &http.Client{Transport: sleepRoundTripper(func(req *http.Request) (*http.Response, error) {
		seen[req.URL.RequestURI()] = true
		body := `{"exists":true,"latest":{"valid":true,"times_woke_up":0,"sleep_interventions":2,"total_awake_time":120,"total_sleep_time":3600,"ongoing":false,"states":[]}}`
		if strings.Contains(req.URL.Path, "/events/last") {
			body = `{"event":null}`
		}
		if strings.HasSuffix(req.URL.Path, "/events") {
			body = `{"events":[]}`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	c := &NanitClient{SessionStore: session.NewSessionStore()}
	c.SessionStore.UpdateAuth("token", "")

	stats, err := c.TryFetchSleepStatsCtx(context.Background(), "baby")
	if err != nil || !stats.Exists || stats.Latest == nil {
		t.Fatalf("stats: %#v %v", stats, err)
	}
	if stats.Latest.TimesWokeUp == nil || *stats.Latest.TimesWokeUp != 0 {
		t.Fatalf("zero presence lost: %#v", stats.Latest.TimesWokeUp)
	}
	if stats.Latest.Ongoing == nil || *stats.Latest.Ongoing {
		t.Fatalf("false presence lost: %#v", stats.Latest.Ongoing)
	}
	if events, err := c.TryFetchSleepEventsCtx(context.Background(), "baby", 200); err != nil || len(events) != 0 {
		t.Fatalf("events: %#v %v", events, err)
	}
	if event, err := c.TryFetchLastSleepEventCtx(context.Background(), "baby"); err != nil || event != nil {
		t.Fatalf("last: %#v %v", event, err)
	}
	for _, path := range []string{"/babies/baby/stats/latest", "/babies/baby/events?limit=200", "/babies/baby/events/last"} {
		if !seen[path] {
			t.Errorf("missing request %s; saw %#v", path, seen)
		}
	}
}

func TestSleepStatsExistsFalseAndCancellation(t *testing.T) {
	old := myClient
	defer func() { myClient = old }()
	myClient = &http.Client{Transport: sleepRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"exists":false,"latest":null}`)), Header: make(http.Header)}, nil
	})}
	c := &NanitClient{SessionStore: session.NewSessionStore()}
	c.SessionStore.UpdateAuth("token", "")
	stats, err := c.TryFetchSleepStatsCtx(context.Background(), "baby")
	if err != nil || stats.Exists || stats.Latest != nil {
		t.Fatalf("exists=false: %#v %v", stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.TryFetchSleepStatsCtx(ctx, "baby"); err != context.Canceled {
		t.Fatalf("cancel error = %v", err)
	}
}

func TestLastSleepEventContract(t *testing.T) {
	tests := []struct {
		name, body, wantKey string
		wantNil, wantErr    bool
	}{
		{name: "direct live object", body: `{"time":1700000000.25,"key":"REMOVED","updated_at":1700000001.5}`, wantKey: "REMOVED"},
		{name: "wrapped compatibility", body: `{"event":{"time":1700000000.25,"key":"WOKE_UP"}}`, wantKey: "WOKE_UP"},
		{name: "top level null", body: `null`, wantNil: true},
		{name: "wrapped null", body: `{"event":null}`, wantNil: true},
		{name: "empty object", body: `{}`, wantNil: true},
		{name: "ambiguous", body: `{"time":1700000000,"key":"REMOVED","event":{"time":1700000001,"key":"WOKE_UP"}}`, wantErr: true},
		{name: "missing time", body: `{"key":"REMOVED"}`, wantErr: true},
		{name: "array", body: `[]`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := myClient
			defer func() { myClient = old }()
			myClient = &http.Client{Transport: sleepRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tt.body)), Header: make(http.Header)}, nil
			})}
			c := &NanitClient{SessionStore: session.NewSessionStore()}
			c.SessionStore.UpdateAuth("token", "")
			event, err := c.TryFetchLastSleepEventCtx(context.Background(), "baby")
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if tt.wantNil {
				if event != nil {
					t.Fatalf("event = %#v, want nil", event)
				}
				return
			}
			if event == nil || event.Key != tt.wantKey {
				t.Fatalf("event = %#v, want key %q", event, tt.wantKey)
			}
			got, ok := event.Time()
			if !ok || got.Nanosecond() != 250000000 || got.Location() != time.UTC {
				t.Fatalf("time = %v, ok=%v", got, ok)
			}
		})
	}
}

func TestSleepReportFieldContract(t *testing.T) {
	fields := []string{"date", "period_date", "period_part", "kind", "valid", "sealed", "ongoing", "timerange", "updated_at", "sleep_start_time", "sleep_end_time", "bed_start_time", "last_wake_up", "sleep_onset", "longest_sleep", "longest_sleep_details", "total_awake_time", "total_sleep_time", "total_present_time", "time_in_bed", "sleep_interventions", "parent_interventions", "soothing_events", "times_out_of_crib", "times_woke_up", "sleep_sessions", "num_events", "sleep_quality", "sleep_score", "night_score", "sleep_score_updated_at", "reports", "states", "sleep_sessions_details", "corrections", "awake_approach_latency", "thresholds", "achievements", "media_urls", "read_by", "seen_by", "shown_by"}
	if len(fields) != 42 {
		t.Fatalf("field contract has %d names", len(fields))
	}
	raw := map[string]interface{}{}
	for _, field := range fields {
		raw[field] = nil
	}
	raw["valid"] = true
	raw["sealed"] = false
	raw["ongoing"] = false
	raw["times_woke_up"] = 0
	raw["time_in_bed"] = map[string]interface{}{"total": 0}
	raw["sleep_score"] = map[string]interface{}{"score": 80, "rating": 3, "version": 1, "metrics": map[string]interface{}{"private": "canary"}}
	raw["night_score"] = map[string]interface{}{"total": 3, "description": "fine", "criteria": map[string]bool{"bedtime": true}}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stats SleepStats
	if err := json.Unmarshal(b, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Valid == nil || !*stats.Valid || stats.Sealed == nil || *stats.Sealed || stats.TimesWokeUp == nil || *stats.TimesWokeUp != 0 {
		t.Fatalf("presence contract lost: %#v", stats)
	}
	if stats.TimeInBed == nil || stats.TimeInBed.Total == nil || stats.TimeInBed.Total.String() != "0" {
		t.Fatalf("nested zero lost: %#v", stats.TimeInBed)
	}
	if stats.SleepScore == nil || stats.SleepScore.Score == nil || stats.SleepScore.Score.String() != "80" || stats.NightScore == nil {
		t.Fatalf("selected nested fields lost: %#v %#v", stats.SleepScore, stats.NightScore)
	}
	for _, malformed := range []string{`{"times_woke_up":"zero"}`, `{"time_in_bed":{"total":"bad"}}`, `{"sleep_score":{"score":"bad"}}`} {
		if err := json.Unmarshal([]byte(malformed), &SleepStats{}); err == nil {
			t.Fatalf("accepted malformed selected field: %s", malformed)
		}
	}
}

func TestSleepEventsTolerateStringTimestamps(t *testing.T) {
	old := myClient
	defer func() { myClient = old }()
	// Live Nanit encodes updated_at/time as numbers or quoted strings.
	body := `{"events":[{"key":"WOKE_UP","time":1789736173.5,"updated_at":"1789740925","internal_key":"VISIT_WOKE_UP"},{"key":"REMOVED","time":"1789738088.812","updated_at":"2026-09-18T13:28:08Z"},null,{"key":"BROKEN","time":false}]}`
	myClient = &http.Client{Transport: sleepRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	c := &NanitClient{SessionStore: session.NewSessionStore()}
	c.SessionStore.UpdateAuth("token", "")
	events, err := c.TryFetchSleepEventsCtx(context.Background(), "baby", 200)
	if err != nil {
		t.Fatalf("string-timestamp events failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (null skipped, malformed skipped): %#v", len(events), events)
	}
	if events[0].UpdatedAt == nil || *events[0].UpdatedAt != 1789740925 {
		t.Fatalf("numeric-string updated_at not parsed: %#v", events[0].UpdatedAt)
	}
	if tm, ok := events[1].Time(); !ok || tm.Year() != 2026 {
		t.Fatalf("string time not parsed: %#v %v", events[1].TimeRaw, tm)
	}
	// Unparsable strings degrade to nil rather than failing the poll.
	var e SleepEvent
	if err := json.Unmarshal([]byte(`{"key":"X","time":1,"updated_at":"not-a-number-or-date"}`), &e); err != nil {
		t.Fatalf("unparsable updated_at should not error: %v", err)
	}
	if e.UpdatedAt != nil {
		t.Fatalf("expected nil updated_at, got %v", *e.UpdatedAt)
	}
}
