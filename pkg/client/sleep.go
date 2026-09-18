package client

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	SleepStateAsleep             = "ASLEEP"
	SleepStateAwake              = "AWAKE"
	SleepStateAbsent             = "ABSENT"
	SleepStateParentIntervention = "PARENT_INTERVENTION"
	SleepStateUnknown            = "UNKNOWN"
	SleepEventKeyFellAsleep      = "FELL_ASLEEP"
	SleepEventKeyWokeUp          = "WOKE_UP"
	SleepEventKeyPutInBed        = "PUT_IN_BED"
	SleepEventKeyPutToSleep      = "PUT_TO_SLEEP"
	SleepEventKeyRemoved         = "REMOVED"
	SleepEventKeyRemovedAsleep   = "REMOVED_ASLEEP"
	SleepEventKeyVisit           = "VISIT"
)

type SleepState struct {
	Title    string   `json:"title"`
	BeginTS  *float64 `json:"begin_ts"`
	EndTS    *float64 `json:"end_ts"`
	UID      string   `json:"uid,omitempty"`
	BabyUID  string   `json:"baby_uid,omitempty"`
	TimeRaw  *float64 `json:"time,omitempty"`
	Obsolete *bool    `json:"obsolete,omitempty"`
}

type SleepTimeRange struct {
	Start *json.Number `json:"start"`
	End   *json.Number `json:"end"`
}

type SleepTimeInBed struct {
	Total *json.Number `json:"total"`
}

type SleepScore struct {
	Score   *json.Number `json:"score"`
	Rating  *json.Number `json:"rating"`
	Version *json.Number `json:"version"`
}

type NightScore struct {
	Total       *json.Number    `json:"total"`
	Description *string         `json:"description"`
	Criteria    map[string]bool `json:"criteria"`
}

type SleepStats struct {
	Valid               *bool           `json:"valid"`
	Sealed              *bool           `json:"sealed"`
	SleepInterventions  *int            `json:"sleep_interventions"`
	TotalAwakeTime      *int            `json:"total_awake_time"`
	TimesWokeUp         *int            `json:"times_woke_up"`
	Date                *string         `json:"date"`
	PeriodDate          *string         `json:"period_date"`
	PeriodPart          *string         `json:"period_part"`
	BedStartTime        *json.Number    `json:"bed_start_time"`
	NumEvents           *int            `json:"num_events"`
	LongestSleep        *int            `json:"longest_sleep"`
	TimesOutOfCrib      *int            `json:"times_out_of_crib"`
	Ongoing             *bool           `json:"ongoing"`
	TotalSleepTime      *int            `json:"total_sleep_time"`
	SleepEndTime        *json.Number    `json:"sleep_end_time"`
	SleepStartTime      *json.Number    `json:"sleep_start_time"`
	SleepOnset          *json.Number    `json:"sleep_onset"`
	TotalPresentTime    *json.Number    `json:"total_present_time"`
	Kind                *string         `json:"kind"`
	LastWakeUp          *json.Number    `json:"last_wake_up"`
	SleepSessions       *json.Number    `json:"sleep_sessions"`
	ParentInterventions *json.Number    `json:"parent_interventions"`
	SoothingEvents      *json.Number    `json:"soothing_events"`
	SleepQuality        *float64        `json:"sleep_quality"`
	SleepScore          *SleepScore     `json:"sleep_score"`
	NightScore          *NightScore     `json:"night_score"`
	SleepScoreUpdatedAt *json.Number    `json:"sleep_score_updated_at"`
	UpdatedAt           *json.Number    `json:"updated_at"`
	TimeInBed           *SleepTimeInBed `json:"time_in_bed"`
	Timerange           *SleepTimeRange `json:"timerange"`
	States              []SleepState    `json:"states"`
}

type SleepStatsResponse struct {
	Exists bool        `json:"exists"`
	Latest *SleepStats `json:"latest"`
}

type SleepEvent struct {
	Key         string   `json:"key"`
	InternalKey string   `json:"internal_key"`
	Title       *string  `json:"title"`
	TimeRaw     *float64 `json:"time"`
	BeginTS     *float64 `json:"begin_ts"`
	EndTS       *float64 `json:"end_ts"`
	BabyUID     string   `json:"baby_uid"`
	CameraUID   string   `json:"camera_uid"`
	UID         string   `json:"uid"`
	Confidence  *float64 `json:"confidence"`
	Source      *string  `json:"source"`
	UpdatedAt   *float64 `json:"updated_at"`
}

// Nanit sometimes encodes unix timestamps as JSON numbers and sometimes as
// quoted strings (numeric or RFC3339). parseUnixFloatPtr accepts a number, a
// numeric string, an RFC3339 string, or null and normalizes to unix seconds.
// Unparsable strings degrade to nil so one odd field cannot fail a whole poll.
func parseUnixFloatPtr(raw json.RawMessage) (*float64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil, err
		}
		s = strings.TrimSpace(s)
		if s == "" || strings.EqualFold(s, "null") {
			return nil, nil
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			v := f
			return &v, nil
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, s); err == nil {
				v := float64(t.UnixNano()) / float64(time.Second)
				return &v, nil
			}
		}
		return nil, nil
	}
	var n json.Number
	if err := json.Unmarshal(trimmed, &n); err != nil {
		return nil, err
	}
	f, err := n.Float64()
	if err != nil {
		return nil, err
	}
	v := f
	return &v, nil
}

func stringPtr(raw json.RawMessage) (*string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return nil, err
		}
		return &s, nil
	}
	// Tolerate numbers/bools in nominally-string fields.
	var v interface{}
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return nil, err
	}
	switch t := v.(type) {
	case string:
		return &t, nil
	case float64:
		s := strconv.FormatFloat(t, 'f', -1, 64)
		return &s, nil
	case bool:
		s := strconv.FormatBool(t)
		return &s, nil
	}
	return nil, nil
}

func plainString(raw json.RawMessage) string {
	s, err := stringPtr(raw)
	if err != nil || s == nil {
		return ""
	}
	return *s
}

func boolPtr(raw json.RawMessage) (*bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var b bool
	if err := json.Unmarshal(trimmed, &b); err == nil {
		return &b, nil
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err == nil {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "true", "1":
			v := true
			return &v, nil
		case "false", "0", "":
			v := false
			return &v, nil
		}
		return nil, nil
	}
	var n json.Number
	if err := json.Unmarshal(trimmed, &n); err == nil {
		if f, err := n.Float64(); err == nil {
			v := f != 0
			return &v, nil
		}
	}
	return nil, nil
}

// UnmarshalJSON keeps the *float64 API while tolerating Nanit's mixed
// number/string timestamp encoding.
func (e *SleepEvent) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*e = SleepEvent{}
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return err
	}
	var err error
	e.Key = plainString(fields["key"])
	e.InternalKey = plainString(fields["internal_key"])
	if e.Title, err = stringPtr(fields["title"]); err != nil {
		return err
	}
	if e.TimeRaw, err = parseUnixFloatPtr(fields["time"]); err != nil {
		return err
	}
	if e.BeginTS, err = parseUnixFloatPtr(fields["begin_ts"]); err != nil {
		return err
	}
	if e.EndTS, err = parseUnixFloatPtr(fields["end_ts"]); err != nil {
		return err
	}
	e.BabyUID = plainString(fields["baby_uid"])
	e.CameraUID = plainString(fields["camera_uid"])
	e.UID = plainString(fields["uid"])
	if e.Confidence, err = parseUnixFloatPtr(fields["confidence"]); err != nil {
		return err
	}
	if e.Source, err = stringPtr(fields["source"]); err != nil {
		return err
	}
	if e.UpdatedAt, err = parseUnixFloatPtr(fields["updated_at"]); err != nil {
		return err
	}
	return nil
}

// UnmarshalJSON applies the same timestamp tolerance to report state intervals.
func (s *SleepState) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*s = SleepState{}
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return err
	}
	var err error
	s.Title = plainString(fields["title"])
	if s.BeginTS, err = parseUnixFloatPtr(fields["begin_ts"]); err != nil {
		return err
	}
	if s.EndTS, err = parseUnixFloatPtr(fields["end_ts"]); err != nil {
		return err
	}
	s.UID = plainString(fields["uid"])
	s.BabyUID = plainString(fields["baby_uid"])
	if s.TimeRaw, err = parseUnixFloatPtr(fields["time"]); err != nil {
		return err
	}
	if s.Obsolete, err = boolPtr(fields["obsolete"]); err != nil {
		return err
	}
	return nil
}

func (e SleepEvent) Time() (time.Time, bool) {
	if e.TimeRaw == nil || math.IsNaN(*e.TimeRaw) || math.IsInf(*e.TimeRaw, 0) || *e.TimeRaw <= 0 {
		return time.Time{}, false
	}
	seconds, fraction := math.Modf(*e.TimeRaw)
	t := time.Unix(int64(seconds), int64(math.Round(fraction*float64(time.Second)))).UTC()
	return t, t.Year() >= 1 && t.Year() <= 9999
}
