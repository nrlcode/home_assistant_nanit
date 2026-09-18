package client

import (
	"encoding/json"
	"math"
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

func (e SleepEvent) Time() (time.Time, bool) {
	if e.TimeRaw == nil || math.IsNaN(*e.TimeRaw) || math.IsInf(*e.TimeRaw, 0) || *e.TimeRaw <= 0 {
		return time.Time{}, false
	}
	seconds, fraction := math.Modf(*e.TimeRaw)
	t := time.Unix(int64(seconds), int64(math.Round(fraction*float64(time.Second)))).UTC()
	return t, t.Year() >= 1 && t.Year() <= 9999
}
