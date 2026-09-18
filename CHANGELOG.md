# Changelog

## Unreleased

- Sleep expansion: latest-report sensors (`longest_sleep`, `sleep_onset`,
  `total_present_time`, `time_in_bed`, `parent_interventions`,
  `soothing_events`, `times_out_of_crib`, `sleep_sessions`, `sleep_score`,
  `sleep_efficiency`, `bed_start_time`, `sleep_start_time`, `sleep_end_time`,
  `last_wake_up`, `sleep_report_status`, `sleep_timeline`, `last_fell_asleep`,
  `last_parent_visit`) plus live `is_asleep` / `in_bed` binary sensors.
  Reports are historical snapshots; live state requires fresh ongoing data.
  Startup/backfill/restart duplicates are suppressed; `sleep_event` is
  non-retained best-effort while report/timestamp/timeline snapshots are
  retained and republished. Bounded private `sleep-history` (30 days, 16 MiB
  aggregate); media URLs, viewer metadata, and identifiers are never published.
  See `examples/home-assistant-sleep-dashboard.yaml` and
  `examples/home-assistant-sleep-recorder.yaml`.
- Config version 1.1.0.

## Unreleased (merged)

- Local↔cloud stream failover with persistent broadcaster; unstable-connection
  backoff resets only after >=10s and >=5 packets.
- Deny-first RTMP IP ACL (`rtmp_allowed_presets` default
  `hassio,frigate,localhost`, explicit `rtmp_allowed_ips` for the camera).
- Conditional `stream_url` sensor, measured `is_stream_alive` (never optimistic),
  single message-only notification polling loop (no sleep/statistics).
- Bridge runs as nanit 1000:1000 via gosu; Frigate input documented
  (`rtmp://<host>:1935/local/<baby_uid>`).

## 1.0.0

- Initial release: packages the `home_assistant_nanit` Go bridge as a
  Home Assistant add-on.
- Auto-wires to the Mosquitto add-on (`services: mqtt:need`).
- Home Assistant MQTT discovery: temperature, humidity, motion, sound, night
  mode and stream sensors plus night-light and standby switches, created
  automatically under a device named after the baby.
- One-time 2FA handled out of band via `get-token.sh`.
