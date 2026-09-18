# Sensors

The bridge publishes camera and sleep updates to MQTT. See `NANIT_MQTT_*` in [`.env.sample`](../.env.sample).

Camera topics include:

- `nanit/babies/{baby_uid}/temperature` — degrees Celsius
- `nanit/babies/{baby_uid}/humidity` — percent
- `nanit/babies/{baby_uid}/is_night` — night mode

Sleep discovery adds report, timestamp, status, timeline, and live-state entities
with stable IDs `nanit_{baby_uid}_{object}`:

| Object | Meaning | Unit |
|---|---|---|
| `times_woke_up` | Wake-up count for the latest period | count |
| `sleep_interventions` | Intervention count for the latest period | count |
| `awake_time_today` | Total awake duration from the latest payload | min |
| `sleep_time_today` | Total sleep duration from the latest payload | min |
| `longest_sleep` | Longest sleep stretch in the latest report | s |
| `sleep_onset` | Time to fall asleep in the latest report | s |
| `total_present_time` | Total present time in the latest report | s |
| `time_in_bed` | Total time in bed in the latest report | s |
| `parent_interventions` | Parent interventions in the latest report | count |
| `soothing_events` | Soothing events in the latest report | count |
| `times_out_of_crib` | Times out of crib in the latest report | count |
| `sleep_sessions` | Sleep sessions in the latest report | count |
| `sleep_score` | Nanit sleep score (0–100, not a percent) | score |
| `sleep_efficiency` | Sleep efficiency | % |
| `bed_start_time` | Bed start of the latest report | timestamp |
| `sleep_start_time` | Sleep start of the latest report | timestamp |
| `sleep_end_time` | Sleep end of the latest report | timestamp |
| `last_wake_up` | Last wake-up of the latest report | timestamp |
| `sleep_report_status` | Latest report status (completed/ongoing) | — |
| `sleep_timeline` | Bounded latest-report timeline rows | — |
| `last_fell_asleep` | Last observed fell-asleep timestamp | timestamp |
| `last_parent_visit` | Last observed parent-visit timestamp | timestamp |
| `last_sleep_event` | Latest sleep event key | — |
| `is_asleep` | Current projected asleep state (fresh data only) | boolean |
| `in_bed` | Current projected crib/bed state (fresh data only) | boolean |

Report sensors describe Nanit's latest report snapshot; `is_asleep` / `in_bed`
describe only fresh live state and never infer presence from a completed report.
The API names some duration fields "today", but the bridge does not infer a
calendar or timezone beyond the returned latest-period payload. Seconds-based
durations stay in seconds; `awake_time_today` / `sleep_time_today` are truncated
to whole minutes. Present zero and false values are published; missing or null
values are not replaced with zero or false.

Each sleep entity requires both bridge availability and its sleep-specific
availability topic. Statistics become stale after 15 minutes and event/state data
after 90 seconds. Transport errors retain the previous scalar while availability
eventually becomes offline; successful missing or invalid responses make the
affected family unavailable immediately. State reconciliation uses verified
event/state Unix timestamps and does not treat fetch time as a transition time.
Startup, backfill, restart, and reconnect duplicates are suppressed; live
`sleep_event` notifications are non-retained and best-effort, while retained
report/timestamp/timeline snapshots are republished on the next normal poll.

`/summary` remains excluded because its contract is unresolved. Media/clip URLs,
viewer metadata, and identifiers are never published to MQTT or stored in the
private history. The bridge keeps only allowlisted scalars, normalized intervals,
sanitized event classes, and hashed dedupe keys in a private `sleep-history`
directory beside `session.json` (mode 0700/0600, 30-day retention, 16 MiB
aggregate cap).

Home Assistant discovery attaches bounded freshness/state/event attributes on
dedicated JSON attribute topics. Raw API responses, credentials, camera IDs, baby
IDs, and event history are never written as attributes or logs.

See [Home Assistant setup](./home-assistant.md) and
`examples/home-assistant-sleep-dashboard.yaml` /
`examples/home-assistant-sleep-recorder.yaml`. For MQTT troubleshooting, use
[MQTT Explorer](http://mqtt-explorer.com/).
