# Changelog

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
