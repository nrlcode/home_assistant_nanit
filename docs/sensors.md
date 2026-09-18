# Sensors

The bridge publishes camera, stream, sleep-state, latest-report, event, and historical timeline entities through MQTT discovery. Entity IDs are stable as `nanit_<baby_uid>_<object>` and are grouped on the baby device.

## Camera and stream entities

| Object | Meaning | Unit |
|---|---|---|
| `temperature` | Camera temperature | °C |
| `humidity` | Camera humidity | % |
| `motion` / `sound` | Last detected event timestamp | timestamp |
| `motion_active` / `sound_active` | Current activity | boolean |
| `night` | Night mode | boolean |
| `stream` | Measured stream liveness | boolean |
| `stream_url` | Advertised RTMP URL, only when explicitly configured | — |
| `night_light` / `standby` | Writable camera controls | boolean |

## Sleep entities

The latest-report sensors expose only fields with verified units and semantics. Duration values are seconds unless marked otherwise; `awake_time_today` and `sleep_time_today` are whole minutes. Missing values remain unavailable rather than being changed to zero or false.

| Object | Meaning |
|---|---|
| `times_woke_up`, `sleep_interventions`, `parent_interventions`, `soothing_events`, `times_out_of_crib`, `sleep_sessions` | Report counts |
| `awake_time_today`, `sleep_time_today` | Latest report durations, minutes |
| `longest_sleep`, `sleep_onset`, `total_present_time`, `time_in_bed` | Latest report durations, seconds |
| `sleep_score`, `sleep_efficiency` | Report score and percentage |
| `bed_start_time`, `sleep_start_time`, `sleep_end_time`, `last_wake_up` | Report timestamps |
| `sleep_report_status` | Report status |
| `sleep_timeline` | Current retained timeline key with JSON attributes |
| `last_fell_asleep`, `last_parent_visit`, `last_sleep_event` | Latest event information |
| `is_asleep`, `in_bed` | Reconciled live binary state |
| `sleep_timeline_history` | Date-addressed historical timeline key |
| `sleep_timeline_history_date` | MQTT select for a UTC `YYYY-MM-DD` history date |

The history-date select publishes its selected date retained on `nanit/babies/{baby_uid}/sleep_timeline_history_date`; invalid, future, missing, or pruned dates publish unavailable history state without changing the latest timeline. Its discovery options include `today` as the initial usable selection; valid dates received through the command topic are retained as state.

## Availability and privacy

Bridge availability and the entity-specific sleep availability topic must both be online. Statistics become stale after 15 minutes; event and live-state data become stale after 90 seconds. Transport failures retain the previous scalar until availability becomes offline, while successful missing or invalid responses make the affected family unavailable immediately.

Historical data is stored only in the private `sleep-history` directory. It is bounded to 30 days, 60 reports, 2,000 events per file, and an intentional approved 64 MiB aggregate cap (the larger cap allows the retained historical timeline to cover the supported 30-day window without premature eviction). Files contain sanitized states, metrics, timestamps, and hashed dedupe keys. Raw API responses, credentials, media/clip URLs, viewer metadata, camera IDs, baby IDs, and arbitrary event fields are never published as MQTT attributes or logs.

See [Home Assistant setup](./home-assistant.md), [sleep dashboard example](../examples/home-assistant-sleep-dashboard.yaml), and [sleep recorder example](../examples/home-assistant-sleep-recorder.yaml).