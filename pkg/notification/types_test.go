package notification

import (
	"testing"
)

func TestNotificationTypeDescription(t *testing.T) {
	tests := []struct {
		notifType   NotificationType
		expected    string
		description string
	}{
		{NotificationMotion, "Motion detected", "motion description"},
		{NotificationSound, "Sound detected", "sound description"},
		{NotificationStanding, "Baby is standing", "standing description"},
		{NotificationLeftBed, "Toddler left the bed", "left bed description"},
		{NotificationAlertZone, "Alert zone triggered", "alert zone description"},
		{NotificationTemperature, "Temperature alert", "temperature description"},
		{NotificationHumidity, "Humidity alert", "humidity description"},
		{NotificationBreathing, "Breathing alert", "breathing description"},
		{NotificationCameraOffline, "Camera offline", "camera offline description"},
		{NotificationCameraOnline, "Camera online", "camera online description"},
		{NotificationLowBattery, "Low battery", "low battery description"},
		{NotificationStateChange, "Camera state change", "state change description"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := tt.notifType.Description(); got != tt.expected {
				t.Errorf("Description() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNotificationTypeDescriptionUnknown(t *testing.T) {
	unknown := NotificationType("UNKNOWN_TYPE")
	got := unknown.Description()
	if got != "UNKNOWN_TYPE" {
		t.Errorf("Description() for unknown type = %q, want %q", got, "UNKNOWN_TYPE")
	}
}

func TestNotificationTypeMQTTTopic(t *testing.T) {
	tests := []struct {
		notifType   NotificationType
		expected    string
		description string
	}{
		{NotificationMotion, "motion", "motion topic"},
		{NotificationSound, "sound", "sound topic"},
		{NotificationStanding, "standing", "standing topic"},
		{NotificationLeftBed, "left_bed", "left bed topic"},
		{NotificationAlertZone, "alert_zone", "alert zone topic"},
		{NotificationTemperature, "temperature_alert", "temperature topic"},
		{NotificationHumidity, "humidity_alert", "humidity topic"},
		{NotificationBreathing, "breathing_alert", "breathing topic"},
		{NotificationCameraOffline, "camera_offline", "camera offline topic"},
		{NotificationCameraOnline, "camera_online", "camera online topic"},
		{NotificationLowBattery, "low_battery", "low battery topic"},
		{NotificationStateChange, "state_change", "state change topic"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := tt.notifType.MQTTTopic(); got != tt.expected {
				t.Errorf("MQTTTopic() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNotificationTypeMQTTTopicUnknown(t *testing.T) {
	unknown := NotificationType("SOME_NEW_TYPE")
	got := unknown.MQTTTopic()
	if got != "some_new_type" {
		t.Errorf("MQTTTopic() for unknown type = %q, want %q", got, "some_new_type")
	}
}

func TestNotificationTypeStateTopic(t *testing.T) {
	tests := []struct {
		notifType   NotificationType
		expected    string
		description string
	}{
		{NotificationMotion, "motion_timestamp", "motion state topic"},
		{NotificationSound, "sound_timestamp", "sound state topic"},
		{NotificationStanding, "is_standing", "standing state topic"},
		{NotificationLeftBed, "left_bed", "left bed state topic"},
		{NotificationAlertZone, "alert_zone_triggered", "alert zone state topic"},
		{NotificationTemperature, "temperature_alert", "temperature state topic"},
		{NotificationHumidity, "humidity_alert", "humidity state topic"},
		{NotificationBreathing, "breathing_alert", "breathing state topic"},
		{NotificationCameraOffline, "camera_online", "camera offline state topic"},
		{NotificationCameraOnline, "camera_online", "camera online state topic"},
		{NotificationLowBattery, "low_battery", "low battery state topic"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := tt.notifType.StateTopic(); got != tt.expected {
				t.Errorf("StateTopic() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNotificationTypeIsBooleanState(t *testing.T) {
	boolTypes := []NotificationType{
		NotificationStanding,
		NotificationLeftBed,
		NotificationAlertZone,
		NotificationTemperature,
		NotificationHumidity,
		NotificationBreathing,
		NotificationCameraOffline,
		NotificationCameraOnline,
		NotificationLowBattery,
	}

	timestampTypes := []NotificationType{
		NotificationMotion,
		NotificationSound,
	}

	for _, nt := range boolTypes {
		if !nt.IsBooleanState() {
			t.Errorf("%s should be boolean state", nt)
		}
	}

	for _, nt := range timestampTypes {
		if nt.IsBooleanState() {
			t.Errorf("%s should NOT be boolean state", nt)
		}
	}
}

func TestNotificationTypeStateValue(t *testing.T) {
	// Boolean true states
	trueStates := []NotificationType{
		NotificationStanding,
		NotificationLeftBed,
		NotificationAlertZone,
		NotificationTemperature,
		NotificationHumidity,
		NotificationBreathing,
		NotificationCameraOnline,
		NotificationLowBattery,
	}

	for _, nt := range trueStates {
		if nt.StateValue() != true {
			t.Errorf("%s.StateValue() should be true", nt)
		}
	}

	// Camera offline is special - sets camera_online to false
	if NotificationCameraOffline.StateValue() != false {
		t.Errorf("CameraOffline.StateValue() should be false")
	}
}

func TestAllNotificationTypes(t *testing.T) {
	all := AllNotificationTypes()
	if len(all) != 19 {
		t.Errorf("AllNotificationTypes() returned %d types, want 19", len(all))
	}

	// Verify all expected types are present
	expected := map[NotificationType]bool{
		NotificationMotion:        true,
		NotificationSound:         true,
		NotificationStanding:      true,
		NotificationLeftBed:       true,
		NotificationAlertZone:     true,
		NotificationTemperature:   true,
		NotificationHumidity:      true,
		NotificationBreathing:     true,
		NotificationCameraOffline: true,
		NotificationCameraOnline:  true,
		NotificationLowBattery:    true,
		NotificationStateChange:   true,
		// Sleep events
		NotificationFellAsleep:    true,
		NotificationWokeUp:        true,
		NotificationPutInBed:      true,
		NotificationPutToSleep:    true,
		NotificationRemoved:       true,
		NotificationRemovedAsleep: true,
		NotificationVisit:         true,
	}

	for _, nt := range all {
		if !expected[nt] {
			t.Errorf("Unexpected notification type: %s", nt)
		}
		delete(expected, nt)
	}

	if len(expected) > 0 {
		for nt := range expected {
			t.Errorf("Missing notification type: %s", nt)
		}
	}
}

func TestSleepEventNotificationTypes(t *testing.T) {
	sleepTypes := SleepEventNotificationTypes()
	if len(sleepTypes) != 7 {
		t.Errorf("SleepEventNotificationTypes() returned %d types, want 7", len(sleepTypes))
	}

	expected := map[NotificationType]bool{
		NotificationFellAsleep:    true,
		NotificationWokeUp:        true,
		NotificationPutInBed:      true,
		NotificationPutToSleep:    true,
		NotificationRemoved:       true,
		NotificationRemovedAsleep: true,
		NotificationVisit:         true,
	}

	for _, nt := range sleepTypes {
		if !expected[nt] {
			t.Errorf("Unexpected sleep notification type: %s", nt)
		}
	}
}

func TestSleepEventStateValue(t *testing.T) {
	tests := []struct {
		notifType NotificationType
		expected  bool
	}{
		{NotificationFellAsleep, true},     // is_asleep = true
		{NotificationWokeUp, false},        // is_asleep = false
		{NotificationPutInBed, true},       // in_bed = true
		{NotificationPutToSleep, true},     // in_bed = true
		{NotificationRemoved, false},       // in_bed = false
		{NotificationRemovedAsleep, false}, // in_bed = false
	}

	for _, tt := range tests {
		t.Run(string(tt.notifType), func(t *testing.T) {
			got := tt.notifType.StateValue()
			if got != tt.expected {
				t.Errorf("StateValue() = %v, want %v", got, tt.expected)
			}
		})
	}
}
