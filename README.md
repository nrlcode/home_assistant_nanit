# Nanit Bridge (merged fork)

Home Assistant add-on + Go bridge for a Nanit baby monitor: a local RTMP
camera feed plus MQTT sensors/controls with auto-discovery, and a
Frigate-ready RTMP input.

This fork merges three lines onto the indiefan base:

| Source | Commit | What it contributes |
|---|---|---|
| indiefan (base) | `d01a3cb` | Original bridge, session handling |
| WColan | `35fd9d0` | HA add-on repo layout, 10-entity MQTT discovery (WColan topic/ID shape) |
| scgreenhalgh | `5244d68` | Deny-first RTMP ACL, local↔cloud failover, unstable-connection backoff, message polling, `stream_url` sensor, non-root runtime |

Module stays `github.com/indiefan/home_assistant_nanit`; the image builds the
vendored local source, never a remote clone. Upstream MIT notices are
preserved (`LICENSE`).

## Install as a Home Assistant add-on (recommended)

1. Add this repository URL in HA under Settings → Add-ons → ⋮ →
   Repositories.
2. Install the Mosquitto broker add-on first — the bridge wires itself to it
   automatically (`services: mqtt:need`), no MQTT config needed.
3. Install "Nanit Bridge", open Configuration, and set:
   - `nanit_refresh_token` — see Authentication below.
   - Leave `rtmp_addr` blank for sensors-only mode to start with.
   - Keep `rtmp_allowed_presets: hassio,frigate,localhost`; add your camera's
     exact observed peer IP to `rtmp_allowed_ips` before enabling RTMP.
4. Start the add-on. On first run, check the log for your `baby_uid`.

Full option reference, ACL rules, Frigate config, SCG migration table, and
rollback steps: see [`DOCS.md`](DOCS.md).

## Install as plain Docker

```bash
docker build -t nanit-bridge .
docker run -d --name=nanit --restart unless-stopped \
  -v /path/to/data:/data \
  -e NANIT_REFRESH_TOKEN=<token> \
  -e NANIT_RTMP_ADDR=192.168.1.100:1935 \
  -e NANIT_MQTT_ENABLED=true \
  -e NANIT_MQTT_BROKER_URL=mqtt://<broker>:1883 \
  -e NANIT_EVENTS_POLLING=true \
  -p 1935:1935 \
  nanit-bridge
```

Use your LAN IP reachable from the camera for `NANIT_RTMP_ADDR` (not
`127.0.0.1`, not the camera's IP). Omit `NANIT_RTMP_ADDR` and set
`NANIT_RTMP_ENABLED=false` for sensors-only. On first run, omit `-d` to read
your `baby_uid` from the logs, then restart detached.

Then add the camera in `configuration.yaml`:

```yaml
camera:
  - platform: ffmpeg
    name: Nanit
    input: "rtmp://192.168.1.100:1935/local/<baby_uid>"
```

## Authentication

Nanit requires 2FA, so the first refresh token is obtained out of band. On any
machine with `bash`, `curl` and `jq`:

```bash
curl -sSL https://raw.githubusercontent.com/WColan/home_assistant_nanit/main/nanit/get-token.sh | bash
```

Enter your Nanit email/password, then the code emailed to you. It prints a
**refresh token** — paste it into the add-on config (`nanit_refresh_token`)
or `NANIT_REFRESH_TOKEN`.

After that it is hands-off: the bridge renews the access token itself about
every 60 minutes and on every 401, persisting the rotated pair to
`/data/session.json` (mode 0600). Re-paste a fresh token only when Nanit
invalidates the refresh token.

> The refresh token grants full access to your Nanit account. Treat it like a
> password. Never commit `session.json`, tokens, or credentials.

## Sleep reports and timeline

The date selector `sleep_timeline_history_date` accepts a validated UTC `YYYY-MM-DD` command on `nanit/babies/<baby>/sleep_timeline_history_date/set` and publishes the matching retained historical snapshot. Missing, future, invalid, or pruned dates publish unavailable state without changing the latest timeline.


MQTT discovery publishes latest-report metrics separately from the fresh live
`is_asleep` and `in_bed` sensors. Completed reports remain historical snapshots
and never imply current state. Sleep-event notifications are best-effort,
non-retained, startup/backfill suppressed, and may be lost during an MQTT
disconnect; retained report, timestamp, status, and timeline snapshots are
republished on the next normal poll.

New entities include `longest_sleep`, `sleep_onset`, `total_present_time`,
`time_in_bed`, `parent_interventions`, `soothing_events`, `times_out_of_crib`,
`sleep_sessions`, `sleep_score` (0–100), `sleep_efficiency`, `bed_start_time`,
`sleep_start_time`, `sleep_end_time`, `last_wake_up`, `sleep_report_status`,
`sleep_timeline`, `last_fell_asleep`, and `last_parent_visit`, plus live
`is_asleep` / `in_bed`. Full topic table: see [`docs/sensors.md`](docs/sensors.md).

The bridge stores only allowlisted report scalars, normalized state intervals,
sanitized event classes, and hashed dedupe keys in a private `sleep-history`
directory beside `session.json`. Files are mode 0600 in a mode 0700 directory,
retained for 30 days, and bounded to 64 MiB aggregate so the supported history
window is not evicted prematurely. It never stores media
URLs, viewer metadata, raw reports, raw corrections, or identity fields. Remove
this owned directory only after making any desired private backup; do not remove
`session.json`.

Use `examples/home-assistant-sleep-dashboard.yaml` as a core-card dashboard
starting point and `examples/home-assistant-sleep-recorder.yaml` to exclude the
bounded timeline attributes from Recorder. Replace the placeholder discovery
suffix first. Times are UTC on MQTT and localized by Home Assistant.

Rollback to an older image leaves the private history directory ignored. To clean
retained MQTT data for this feature only, remove the discovery/state/availability
topics for the entities above plus `sleep_event` (non-retained). Preserve the
pre-existing sleep and stream topics.

## Notes

- The camera has one local RTMP slot. If the Nanit app is streaming locally at
  the same time, one of the two will drop.
- Empty RTMP presets/IPs deny everything; malformed rules fail startup
  instead of falling open. Never expose the RTMP port to the internet — the
  ACL is convenience allowlisting, not authentication or encryption.
- Sensor updates arrive every few minutes and on change; motion/sound stay
  active ~45 s per latest event.
- Session file format is `Revision=3`; no irreversible migration exists.
  Rollback is: stop the new publisher, restore the previous image/options,
  and clean or restore retained discovery configs.

## Development

```bash
go build ./...
go test ./... -count=1
go test -race ./...
go vet ./...
bash -n rootfs/run.sh
```

## License

MIT — see [`LICENSE`](LICENSE).
