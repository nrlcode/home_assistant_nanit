package mqtt

import "testing"

func TestSleepDiscoveryHasSevenStableIDsAndDualAvailability(t *testing.T) {
	want := map[string]string{
		"times_woke_up": "sensor", "sleep_interventions": "sensor", "awake_time_today": "sensor",
		"sleep_time_today": "sensor", "last_sleep_event": "sensor", "is_asleep": "binary_sensor", "in_bed": "binary_sensor",
	}
	for object, component := range want {
		var found *entitySpec
		for i := range entitySpecs {
			if entitySpecs[i].object == object {
				found = &entitySpecs[i]
				break
			}
		}
		if found == nil || found.component != component {
			t.Errorf("%s missing/wrong: %#v", object, found)
			continue
		}
		if found.sleepAvailability == "" {
			t.Errorf("%s missing sleep availability", object)
		}
	}
}

func TestSleepDiscoveryUnits(t *testing.T) {
	for _, spec := range entitySpecs {
		if spec.object == "awake_time_today" || spec.object == "sleep_time_today" {
			if spec.unit != "min" {
				t.Errorf("%s unit = %q", spec.object, spec.unit)
			}
		}
	}
}

func TestExpandedSleepDiscovery(t *testing.T) {
	want := map[string]string{
		"longest_sleep": "s", "sleep_onset": "s", "total_present_time": "s", "time_in_bed": "s",
		"parent_interventions": "", "soothing_events": "", "times_out_of_crib": "", "sleep_sessions": "",
		"sleep_score": "", "sleep_efficiency": "%", "bed_start_time": "", "sleep_start_time": "",
		"sleep_end_time": "", "last_wake_up": "", "sleep_report_status": "", "sleep_timeline": "",
		"last_fell_asleep": "", "last_parent_visit": "",
	}
	for object, unit := range want {
		var found *entitySpec
		for i := range entitySpecs {
			if entitySpecs[i].object == object {
				found = &entitySpecs[i]
				break
			}
		}
		if found == nil {
			t.Errorf("missing %s", object)
			continue
		}
		if found.unit != unit {
			t.Errorf("%s unit=%q", object, found.unit)
		}
		if found.stateCls != "" {
			t.Errorf("%s state_class=%q", object, found.stateCls)
		}
	}
}
