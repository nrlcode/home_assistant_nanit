# Sensors

The bridge publishes camera and sleep updates to MQTT. See `NANIT_MQTT_*` in [`.env.sample`](../.env.sample).

Camera topics include:

- `nanit/babies/{baby_uid}/temperature` — degrees Celsius
- `nanit/babies/{baby_uid}/humidity` — percent
- `nanit/babies/{baby_uid}/is_night` — night mode

Sleep discovery adds five sensors and two binary sensors with stable IDs `nanit_{baby_uid}_{object}`:

| Object | Meaning | Unit |
|---|---|---|
| `times_woke_up` | Wake-up count for the API's latest period | count |
| `sleep_interventions` | Intervention count for the latest period | count |
| `awake_time_today` | Total awake duration from the latest payload | min |
| `sleep_time_today` | Total sleep duration from the latest payload | min |
| `last_sleep_event` | Latest sleep event key | — |
| `is_asleep` | Current projected asleep state | boolean |
| `in_bed` | Current projected crib/bed state | boolean |

The API names the duration fields “today”, but the bridge does not infer a calendar or timezone beyond the returned latest-period payload. Seconds are truncated to whole minutes. Present zero and false values are published; missing or null values are not replaced with zero or false.

Each sleep entity requires both bridge availability and its sleep-specific availability topic. Statistics become stale after 15 minutes and event/state data after 90 seconds. Transport errors retain the previous scalar while availability eventually becomes offline; successful missing or invalid responses make the affected family unavailable immediately. State reconciliation uses verified event/state Unix timestamps and does not treat fetch time as a transition time.

Only the four statistics above are exposed. Other `/stats/latest` fields remain diagnostic-only because their units or semantics are not verified. Historical state arrays, arbitrary event fields, media/clip URLs, crying detection, daily summary, and account-access assumptions are not exposed. `/summary` remains excluded because its contract is unresolved.

Home Assistant discovery attaches bounded freshness/state/event attributes on dedicated JSON attribute topics. Raw API responses, credentials, camera IDs, baby IDs, and event history are never written as attributes or logs.

See [Home Assistant setup](./home-assistant.md). For MQTT troubleshooting, use [MQTT Explorer](http://mqtt-explorer.com/).
