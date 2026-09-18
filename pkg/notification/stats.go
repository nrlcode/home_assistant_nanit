package notification

import "github.com/indiefan/home_assistant_nanit/pkg/client"

const (
	SleepStateAsleep             = client.SleepStateAsleep
	SleepStateAwake              = client.SleepStateAwake
	SleepStateAbsent             = client.SleepStateAbsent
	SleepStateParentIntervention = client.SleepStateParentIntervention
	SleepStateUnknown            = client.SleepStateUnknown
)

type SleepState = client.SleepState
type SleepStats = client.SleepStats
type SleepStatsResponse = client.SleepStatsResponse

func currentSleepState(s *SleepStats) *SleepState {
	if s == nil || len(s.States) == 0 {
		return nil
	}
	best := &s.States[0]
	for i := 1; i < len(s.States); i++ {
		if sleepStateOrder(s.States[i]) >= sleepStateOrder(*best) {
			best = &s.States[i]
		}
	}
	return best
}

func sleepStateOrder(s SleepState) float64 {
	if s.TimeRaw != nil {
		return *s.TimeRaw
	}
	if s.BeginTS != nil {
		return *s.BeginTS
	}
	return 0
}
