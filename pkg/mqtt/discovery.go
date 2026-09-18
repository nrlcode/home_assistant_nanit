package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/rs/zerolog/log"
)

// Home Assistant MQTT discovery.
//
// On (re)connect we publish a retained discovery config for every entity of
// every known baby to `<discovery_prefix>/<component>/nanit_<uid>/<object>/config`.
// Home Assistant then creates a "Nanit" device with all of the sensors and the
// two writable controls (night light, standby) automatically - no YAML.

type haDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer"`
	Model        string   `json:"model"`
}

type haAvailability struct {
	Topic               string `json:"topic"`
	PayloadAvailable    string `json:"payload_available"`
	PayloadNotAvailable string `json:"payload_not_available"`
}

type haEntity struct {
	Name                string           `json:"name"`
	UniqueID            string           `json:"unique_id"`
	StateTopic          string           `json:"state_topic"`
	CommandTopic        string           `json:"command_topic,omitempty"`
	DeviceClass         string           `json:"device_class,omitempty"`
	StateClass          string           `json:"state_class,omitempty"`
	UnitOfMeasurement   string           `json:"unit_of_measurement,omitempty"`
	Icon                string           `json:"icon,omitempty"`
	PayloadOn           string           `json:"payload_on,omitempty"`
	PayloadOff          string           `json:"payload_off,omitempty"`
	StateOn             string           `json:"state_on,omitempty"`
	StateOff            string           `json:"state_off,omitempty"`
	Availability        []haAvailability `json:"availability,omitempty"`
	AvailabilityMode    string           `json:"availability_mode,omitempty"`
	JSONAttributesTopic string           `json:"json_attributes_topic,omitempty"`
	Device              haDevice         `json:"device"`
}

type entitySpec struct {
	component         string // "sensor" | "binary_sensor" | "switch"
	object            string // object_id suffix
	friendly          string
	topic             string // state topic key (under nanit/babies/<uid>/)
	cmd               string // command topic key, for switches
	class             string
	stateCls          string
	unit              string
	icon              string
	sleepAvailability string
	attributes        string
}

var entitySpecs = []entitySpec{
	{component: "sensor", object: "temperature", friendly: "Temperature", topic: "temperature", class: "temperature", stateCls: "measurement", unit: "°C"},
	{component: "sensor", object: "humidity", friendly: "Humidity", topic: "humidity", class: "humidity", stateCls: "measurement", unit: "%"},
	{component: "sensor", object: "motion", friendly: "Last motion", topic: "motion", class: "timestamp", icon: "mdi:motion-sensor"},
	{component: "sensor", object: "sound", friendly: "Last sound", topic: "sound", class: "timestamp", icon: "mdi:ear-hearing"},
	{component: "binary_sensor", object: "motion_active", friendly: "Motion", topic: "motion_active", class: "motion"},
	{component: "binary_sensor", object: "sound_active", friendly: "Sound", topic: "sound_active", class: "sound"},
	{component: "binary_sensor", object: "night", friendly: "Night mode", topic: "is_night", icon: "mdi:weather-night"},
	{component: "binary_sensor", object: "stream", friendly: "Stream", topic: "is_stream_alive", class: "connectivity"},
	{component: "switch", object: "night_light", friendly: "Night light", topic: "night_light", cmd: "night_light/switch", icon: "mdi:lightbulb-night"},
	{component: "switch", object: "standby", friendly: "Standby", topic: "standby", cmd: "standby/switch", icon: "mdi:power-standby"},
	{component: "sensor", object: "times_woke_up", friendly: "Times woke up", topic: "times_woke_up", icon: "mdi:sleep-off", sleepAvailability: "times_woke_up", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_interventions", friendly: "Sleep interventions", topic: "sleep_interventions", icon: "mdi:human-greeting-proximity", sleepAvailability: "sleep_interventions", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "awake_time_today", friendly: "Awake time today", topic: "awake_time_today", unit: "min", icon: "mdi:clock-time-four-outline", sleepAvailability: "awake_time_today", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_time_today", friendly: "Sleep time today", topic: "sleep_time_today", unit: "min", icon: "mdi:clock-time-four", sleepAvailability: "sleep_time_today", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "longest_sleep", friendly: "Longest sleep", topic: "longest_sleep", unit: "s", icon: "mdi:timer", sleepAvailability: "longest_sleep", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_onset", friendly: "Sleep onset", topic: "sleep_onset", unit: "s", icon: "mdi:timer-sand", sleepAvailability: "sleep_onset", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "total_present_time", friendly: "Total present time", topic: "total_present_time", unit: "s", icon: "mdi:bed-clock", sleepAvailability: "total_present_time", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "time_in_bed", friendly: "Time in bed", topic: "time_in_bed", unit: "s", icon: "mdi:bed", sleepAvailability: "time_in_bed", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "parent_interventions", friendly: "Parent interventions", topic: "parent_interventions", icon: "mdi:human-greeting-proximity", sleepAvailability: "parent_interventions", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "soothing_events", friendly: "Soothing events", topic: "soothing_events", icon: "mdi:hand-heart", sleepAvailability: "soothing_events", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "times_out_of_crib", friendly: "Times out of crib", topic: "times_out_of_crib", icon: "mdi:bed-empty", sleepAvailability: "times_out_of_crib", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_sessions", friendly: "Sleep sessions", topic: "sleep_sessions", icon: "mdi:counter", sleepAvailability: "sleep_sessions", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_score", friendly: "Sleep score", topic: "sleep_score", icon: "mdi:gauge", sleepAvailability: "sleep_score", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_efficiency", friendly: "Sleep efficiency", topic: "sleep_efficiency", unit: "%", icon: "mdi:percent", sleepAvailability: "sleep_efficiency", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "bed_start_time", friendly: "Bed start time", topic: "bed_start_time", class: "timestamp", sleepAvailability: "bed_start_time", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_start_time", friendly: "Sleep start time", topic: "sleep_start_time", class: "timestamp", sleepAvailability: "sleep_start_time", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_end_time", friendly: "Sleep end time", topic: "sleep_end_time", class: "timestamp", sleepAvailability: "sleep_end_time", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "last_wake_up", friendly: "Last wake up", topic: "last_wake_up", class: "timestamp", sleepAvailability: "last_wake_up", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_report_status", friendly: "Sleep report status", topic: "sleep_report_status", icon: "mdi:file-chart", sleepAvailability: "sleep_report_status", attributes: "sleep_stats_attributes"},
	{component: "sensor", object: "sleep_timeline", friendly: "Sleep timeline", topic: "sleep_timeline", icon: "mdi:timeline-clock", sleepAvailability: "sleep_timeline", attributes: "sleep_timeline_attributes"},
	{component: "sensor", object: "sleep_timeline_history", friendly: "Sleep timeline history", topic: "sleep_timeline_history", icon: "mdi:timeline-clock-outline", sleepAvailability: "sleep_timeline_history", attributes: "sleep_timeline_history_attributes"},
	{component: "select", object: "sleep_timeline_history_date", friendly: "Sleep timeline history date (UTC)", topic: "sleep_timeline_history_date", cmd: "sleep_timeline_history_date/set", icon: "mdi:calendar"},
	{component: "sensor", object: "last_fell_asleep", friendly: "Last fell asleep", topic: "last_fell_asleep", class: "timestamp", sleepAvailability: "last_fell_asleep", attributes: "sleep_event_attributes"},
	{component: "sensor", object: "last_parent_visit", friendly: "Last parent visit", topic: "last_parent_visit", class: "timestamp", sleepAvailability: "last_parent_visit", attributes: "sleep_event_attributes"},
	{component: "sensor", object: "last_sleep_event", friendly: "Last sleep event", topic: "last_sleep_event", icon: "mdi:calendar-clock", sleepAvailability: "last_sleep_event", attributes: "sleep_event_attributes"},
	{component: "binary_sensor", object: "is_asleep", friendly: "Asleep", topic: "is_asleep", icon: "mdi:sleep", sleepAvailability: "is_asleep", attributes: "sleep_state_attributes"},
	{component: "binary_sensor", object: "in_bed", friendly: "In bed", topic: "in_bed", icon: "mdi:bed", sleepAvailability: "in_bed", attributes: "sleep_state_attributes"},
}

// streamURLForBaby returns the advertised rtmp:// URL for a baby, or "" when
// RTMP is disabled or the configured address is not a valid explicit host:port.
func streamURLForBaby(rtmpAddr, babyUID string) string {
	rtmpAddr = strings.TrimSpace(rtmpAddr)
	if rtmpAddr == "" || babyUID == "" {
		return ""
	}
	addr := strings.TrimPrefix(strings.TrimPrefix(rtmpAddr, "rtmp://"), "rtmps://")
	// Require explicit host:port; never guess a container IP.
	parts := strings.Split(addr, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	for _, c := range parts[1] {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return fmt.Sprintf("rtmp://%s/local/%s", addr, babyUID)
}

// publishDiscovery publishes retained HA discovery configs for one baby.
// It returns the number of failed publications; failures are logged and
// retried on the next successful connection, never cached as published.
func (conn *Connection) publishDiscovery(babyUID, babyName string) int {
	if !conn.Opts.DiscoveryEnabled {
		return 0
	}

	discoveryPrefix := conn.Opts.DiscoveryPrefix
	if discoveryPrefix == "" {
		discoveryPrefix = "homeassistant"
	}

	name := babyName
	if name == "" {
		short := babyUID
		if len(short) > 6 {
			short = short[:6]
		}
		name = "Nanit " + short
	}

	dev := haDevice{
		Identifiers:  []string{"nanit_" + babyUID},
		Name:         name,
		Manufacturer: "Nanit",
		Model:        "Baby Monitor",
	}
	avail := []haAvailability{{
		Topic:               fmt.Sprintf("%v/status", conn.Opts.TopicPrefix),
		PayloadAvailable:    "online",
		PayloadNotAvailable: "offline",
	}}

	base := fmt.Sprintf("%v/babies/%v", conn.Opts.TopicPrefix, babyUID)
	failures := 0
	client := conn.GetClient()

	publishOne := func(component, object string, e haEntity) {
		conn.mu.Lock()
		shutdown := conn.shutdown
		conn.mu.Unlock()
		if shutdown {
			return
		}
		payload, err := json.Marshal(e)
		if err != nil {
			log.Error().Err(err).Str("object", object).Msg("Unable to marshal discovery config")
			failures++
			return
		}
		topic := fmt.Sprintf("%v/%v/nanit_%v/%v/config", discoveryPrefix, component, babyUID, object)
		if token := client.Publish(topic, 1, true, payload); token.Wait() && token.Error() != nil {
			log.Error().Err(token.Error()).Str("topic", topic).Msg("Unable to publish discovery config")
			failures++
		} else {
			log.Debug().Str("topic", topic).Msg("Published HA discovery config")
		}
	}

	for _, s := range entitySpecs {
		e := haEntity{
			Name:                s.friendly,
			UniqueID:            fmt.Sprintf("nanit_%v_%v", babyUID, s.object),
			StateTopic:          fmt.Sprintf("%v/%v", base, s.topic),
			DeviceClass:         s.class,
			StateClass:          s.stateCls,
			UnitOfMeasurement:   s.unit,
			Icon:                s.icon,
			Availability:        avail,
			JSONAttributesTopic: "",
			Device:              dev,
		}
		if s.sleepAvailability != "" {
			e.Availability = append(e.Availability, haAvailability{Topic: fmt.Sprintf("%v/%v/availability", base, s.sleepAvailability), PayloadAvailable: "online", PayloadNotAvailable: "offline"})
			e.AvailabilityMode = "all"
			e.JSONAttributesTopic = fmt.Sprintf("%v/%v", base, s.attributes)
		}
		if s.component == "binary_sensor" || s.component == "switch" {
			e.PayloadOn = "true"
			e.PayloadOff = "false"
		}
		if s.component == "switch" || s.component == "select" {
			e.CommandTopic = fmt.Sprintf("%v/%v", base, s.cmd)
		}
		if s.component == "switch" {
			e.StateOn = "true"
			e.StateOff = "false"
		}
		publishOne(s.component, s.object, e)
	}

	// Conditional stream_url sensor, only when RTMP is configured with an
	// explicit valid advertised address.
	if url := streamURLForBaby(conn.Opts.RTMPAddr, babyUID); url != "" {
		e := haEntity{
			Name:         "Stream URL",
			UniqueID:     fmt.Sprintf("nanit_%v_stream_url", babyUID),
			StateTopic:   fmt.Sprintf("%v/stream_url", base),
			Icon:         "mdi:video",
			Availability: avail,
			Device:       dev,
		}
		publishOne("sensor", "stream_url", e)
		// Publish the retained URL value itself.
		conn.mu.Lock()
		shutdown := conn.shutdown
		conn.mu.Unlock()
		if !shutdown {
			topic := fmt.Sprintf("%v/stream_url", base)
			if token := client.Publish(topic, 1, true, url); token.Wait() && token.Error() != nil {
				log.Error().Err(token.Error()).Str("topic", topic).Msg("Unable to publish stream_url value")
				failures++
			}
		}
	}

	// Measured stream state starts false until media is observed; never an
	// optimistic true. A live stream re-affirms via state updates.
	// Always publish the retained boolean so reconnect restores true when alive.
	streamAlive := false
	if conn.StateManager != nil {
		if st := conn.StateManager.GetBabyState(babyUID); st != nil && st.GetStreamState() == baby.StreamState_Alive {
			streamAlive = true
		}
	}
	conn.mu.Lock()
	shutdown := conn.shutdown
	conn.mu.Unlock()
	if !shutdown {
		topic := fmt.Sprintf("%v/is_stream_alive", base)
		val := "false"
		if streamAlive {
			val = "true"
		}
		if token := client.Publish(topic, 0, true, val); token.Wait() && token.Error() != nil {
			log.Error().Err(token.Error()).Str("topic", topic).Msg("Unable to publish stream state")
			failures++
		}
	}

	log.Info().Str("baby", babyUID).Str("name", name).Int("failures", failures).Msg("Published Home Assistant MQTT discovery")
	return failures
}
