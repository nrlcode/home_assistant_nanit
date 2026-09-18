// Package notification provides types and utilities for handling Nanit camera notifications
package notification

import (
	"strings"
)

// NotificationType represents a Nanit notification type
type NotificationType string

const (
	// NotificationMotion - motion detected in camera view
	NotificationMotion NotificationType = "MOTION"

	// NotificationSound - sound detected
	NotificationSound NotificationType = "SOUND"

	// NotificationStanding - baby is standing in crib
	NotificationStanding NotificationType = "BABY_STANDING"

	// NotificationLeftBed - toddler left the bed
	NotificationLeftBed NotificationType = "TODDLER_LEFT_THE_BED"

	// NotificationAlertZone - movement in alert zone
	NotificationAlertZone NotificationType = "ALERT_ZONE"

	// NotificationTemperature - temperature alert
	NotificationTemperature NotificationType = "TEMPERATURE"

	// NotificationHumidity - humidity alert
	NotificationHumidity NotificationType = "HUMIDITY"

	// NotificationBreathing - breathing monitoring alert
	NotificationBreathing NotificationType = "BREATHING_ALERT"

	// NotificationCameraOffline - camera went offline
	NotificationCameraOffline NotificationType = "CAMERA_OFFLINE"

	// NotificationCameraOnline - camera came online
	NotificationCameraOnline NotificationType = "CAMERA_ONLINE"

	// NotificationLowBattery - low battery warning
	NotificationLowBattery NotificationType = "LOW_BATTERY"

	// NotificationStateChange - camera state change
	NotificationStateChange NotificationType = "CHANGE_STATE"

	// Sleep event notification types (from /events API)

	// NotificationFellAsleep - baby fell asleep
	NotificationFellAsleep NotificationType = "FELL_ASLEEP"

	// NotificationWokeUp - baby woke up
	NotificationWokeUp NotificationType = "WOKE_UP"

	// NotificationPutInBed - baby was put in bed
	NotificationPutInBed NotificationType = "PUT_IN_BED"

	// NotificationPutToSleep - baby was put to sleep
	NotificationPutToSleep NotificationType = "PUT_TO_SLEEP"

	// NotificationRemoved - baby was removed from crib
	NotificationRemoved NotificationType = "REMOVED"

	// NotificationRemovedAsleep - baby was removed while asleep
	NotificationRemovedAsleep NotificationType = "REMOVED_ASLEEP"

	// NotificationVisit - parent visit/approaching
	NotificationVisit NotificationType = "VISIT"
)

// descriptions maps notification types to human-readable descriptions
var descriptions = map[NotificationType]string{
	NotificationMotion:        "Motion detected",
	NotificationSound:         "Sound detected",
	NotificationStanding:      "Baby is standing",
	NotificationLeftBed:       "Toddler left the bed",
	NotificationAlertZone:     "Alert zone triggered",
	NotificationTemperature:   "Temperature alert",
	NotificationHumidity:      "Humidity alert",
	NotificationBreathing:     "Breathing alert",
	NotificationCameraOffline: "Camera offline",
	NotificationCameraOnline:  "Camera online",
	NotificationLowBattery:    "Low battery",
	NotificationStateChange:   "Camera state change",
	// Sleep event descriptions
	NotificationFellAsleep:    "Baby fell asleep",
	NotificationWokeUp:        "Baby woke up",
	NotificationPutInBed:      "Baby put in bed",
	NotificationPutToSleep:    "Baby put to sleep",
	NotificationRemoved:       "Baby removed from crib",
	NotificationRemovedAsleep: "Baby removed while asleep",
	NotificationVisit:         "Parent visit",
}

// mqttTopics maps notification types to MQTT event topic suffixes
var mqttTopics = map[NotificationType]string{
	NotificationMotion:        "motion",
	NotificationSound:         "sound",
	NotificationStanding:      "standing",
	NotificationLeftBed:       "left_bed",
	NotificationAlertZone:     "alert_zone",
	NotificationTemperature:   "temperature_alert",
	NotificationHumidity:      "humidity_alert",
	NotificationBreathing:     "breathing_alert",
	NotificationCameraOffline: "camera_offline",
	NotificationCameraOnline:  "camera_online",
	NotificationLowBattery:    "low_battery",
	NotificationStateChange:   "state_change",
	// Sleep event topics
	NotificationFellAsleep:    "fell_asleep",
	NotificationWokeUp:        "woke_up",
	NotificationPutInBed:      "put_in_bed",
	NotificationPutToSleep:    "put_to_sleep",
	NotificationRemoved:       "removed",
	NotificationRemovedAsleep: "removed_asleep",
	NotificationVisit:         "visit",
}

// stateTopics maps notification types to MQTT state topic suffixes
var stateTopics = map[NotificationType]string{
	NotificationMotion:        "motion_timestamp",
	NotificationSound:         "sound_timestamp",
	NotificationStanding:      "is_standing",
	NotificationLeftBed:       "left_bed",
	NotificationAlertZone:     "alert_zone_triggered",
	NotificationTemperature:   "temperature_alert",
	NotificationHumidity:      "humidity_alert",
	NotificationBreathing:     "breathing_alert",
	NotificationCameraOffline: "camera_online", // Note: offline sets camera_online to false
	NotificationCameraOnline:  "camera_online",
	NotificationLowBattery:    "low_battery",
	// Sleep event state topics
	NotificationFellAsleep:    "is_asleep",  // sets to true
	NotificationWokeUp:        "is_asleep",  // sets to false
	NotificationPutInBed:      "in_bed",     // sets to true
	NotificationPutToSleep:    "in_bed",     // sets to true
	NotificationRemoved:       "in_bed",     // sets to false
	NotificationRemovedAsleep: "in_bed",     // sets to false
	NotificationVisit:         "last_event", // timestamp only
}

// booleanStateTypes are notification types that publish boolean state values
var booleanStateTypes = map[NotificationType]bool{
	NotificationStanding:      true,
	NotificationLeftBed:       true,
	NotificationAlertZone:     true,
	NotificationTemperature:   true,
	NotificationHumidity:      true,
	NotificationBreathing:     true,
	NotificationCameraOffline: true,
	NotificationCameraOnline:  true,
	NotificationLowBattery:    true,
	// Sleep events with boolean state
	NotificationFellAsleep:    true,
	NotificationWokeUp:        true,
	NotificationPutInBed:      true,
	NotificationPutToSleep:    true,
	NotificationRemoved:       true,
	NotificationRemovedAsleep: true,
}

// Description returns a human-readable description of the notification type
func (t NotificationType) Description() string {
	if desc, ok := descriptions[t]; ok {
		return desc
	}
	return string(t)
}

// MQTTTopic returns the MQTT topic suffix for this notification type
// Used for event topics: {prefix}/babies/{baby_uid}/events/{topic}
func (t NotificationType) MQTTTopic() string {
	if topic, ok := mqttTopics[t]; ok {
		return topic
	}
	// For unknown types, convert to lowercase snake_case
	return strings.ToLower(strings.ReplaceAll(string(t), " ", "_"))
}

// StateTopic returns the MQTT state topic suffix for this notification type
// Used for state topics: {prefix}/babies/{baby_uid}/{topic}
func (t NotificationType) StateTopic() string {
	if topic, ok := stateTopics[t]; ok {
		return topic
	}
	return ""
}

// IsBooleanState returns true if this notification type publishes a boolean state
// (as opposed to a timestamp)
func (t NotificationType) IsBooleanState() bool {
	return booleanStateTypes[t]
}

// StateValue returns the boolean value to publish for this notification type
// For most types this is true, but some set their state topic to false
func (t NotificationType) StateValue() interface{} {
	switch t {
	case NotificationCameraOffline:
		return false // camera_online = false
	case NotificationWokeUp:
		return false // is_asleep = false
	case NotificationRemoved, NotificationRemovedAsleep:
		return false // in_bed = false
	default:
		return true
	}
}

// AllNotificationTypes returns all known notification types
func AllNotificationTypes() []NotificationType {
	return []NotificationType{
		NotificationMotion,
		NotificationSound,
		NotificationStanding,
		NotificationLeftBed,
		NotificationAlertZone,
		NotificationTemperature,
		NotificationHumidity,
		NotificationBreathing,
		NotificationCameraOffline,
		NotificationCameraOnline,
		NotificationLowBattery,
		NotificationStateChange,
		// Sleep events
		NotificationFellAsleep,
		NotificationWokeUp,
		NotificationPutInBed,
		NotificationPutToSleep,
		NotificationRemoved,
		NotificationRemovedAsleep,
		NotificationVisit,
	}
}

// SleepEventNotificationTypes returns only sleep event related notification types
func SleepEventNotificationTypes() []NotificationType {
	return []NotificationType{
		NotificationFellAsleep,
		NotificationWokeUp,
		NotificationPutInBed,
		NotificationPutToSleep,
		NotificationRemoved,
		NotificationRemovedAsleep,
		NotificationVisit,
	}
}
