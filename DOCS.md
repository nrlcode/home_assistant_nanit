# Nanit Bridge

Runs the [`home_assistant_nanit`](https://github.com/WColan/home_assistant_nanit)
Go bridge as a Home Assistant add-on: a local **RTMP camera feed** plus **MQTT
sensors and controls** for your Nanit baby monitor, created automatically in
Home Assistant via MQTT discovery.

## Entities created

A device named after your baby, with:

| Entity | Type | Notes |
|---|---|---|
| Temperature | sensor | °C from the camera's sensor |
| Humidity | sensor | % RH |
| Last motion / Last sound | sensor | timestamp of the last event |
| Motion / Sound | binary_sensor | on for ~45 s after an event |
| Night mode | binary_sensor | camera day/night state |
| Stream | binary_sensor | RTMP feed health |
| Night light | switch | turns the camera's night light on/off |
| Standby | switch | puts the camera in standby |

The camera itself is added separately as an `ffmpeg` camera pointing at
`rtmp://<rtmp_addr>/local/<baby_uid>` (see below).

## Setup

### 1. Install the Mosquitto broker add-on

This add-on wires itself to Mosquitto automatically — no MQTT config needed here.

### 2. Get a Nanit refresh token

Nanit requires 2FA, so authentication happens once, outside the add-on. On any
machine with `bash`, `curl` and `jq` (your Mac is fine):

```bash
curl -sSL https://raw.githubusercontent.com/WColan/home_assistant_nanit/main/nanit/get-token.sh | bash
```

Enter your Nanit email/password, then the code it emails you. It prints a
**refresh token**. Copy it.

> The refresh token grants full access to your Nanit account. Treat it like a
> password.

### 3. Configure the add-on

| Option | Value |
|---|---|
| `nanit_refresh_token` | the token from step 2 |
| `rtmp_addr` | `<home-assistant-ip>:1935` — an address the **camera** can reach (usually your HA box's LAN IP). Omit for sensors only. |
| `rtmp_allowed_presets` | `"hassio,frigate,localhost"` — HA add-on network, Frigate add-on, loopback. No implicit LAN access. |
| `rtmp_allowed_ips` | Optional comma-separated IPs/CIDRs. Add the camera's exact observed peer IP here (and an external Frigate's IP, if any), otherwise publishing is denied. |
| `mqtt_discovery` | `true` |
| `event_polling` | `true` (needed for motion/sound) |
| `log_level` | `info` |

Start the add-on. On first run, check the log for the line reporting your
`baby_uid` — you'll need it for the camera.

### 4. Add the camera (optional)

In `configuration.yaml`:

```yaml
camera:
  - platform: ffmpeg
    name: Nanit
    input: "rtmp://<rtmp_addr>/local/<baby_uid>"
```

## Packaging and network behavior

The add-on uses host networking because the camera must publish directly to RTMP
port 1935 on the Home Assistant host. The port is declared as `1935/tcp` for
Supervisor metadata, but host networking means there is no separate container
port mapping. Do not expose it to the internet. No Supervisor watchdog is
declared: RTMP is a media protocol, not a valid health-check endpoint, and the
bridge exposes no separate health service.

Backups exclude `/data/video` and `/data/log`; `/data/session.json` remains in
backups so an authenticated session can survive restore. The process still runs
as the non-root `nanit` user, and startup checks that its data paths are writable.

Docker builds support `aarch64`, `amd64`, and `armv7`. `BUILD_ARCH` maps to Go
`arm64`, `amd64`, and `arm` with `GOARM=7`, respectively. The `nanit` slug is
URI-friendly and needs to be unique only within this add-on repository. The
bundled icon and logo provide the repository UI branding.

## Notes

- The camera has one local RTMP slot. If the Nanit app is streaming locally at
  the same time, one of the two will drop.
- If your HA box is on Wi-Fi a hop away from the camera and the RTMP feed won't
  hold, run the RTMP part on a box on the same LAN as the camera and leave
  `rtmp_addr` blank here (MQTT still works).
- Sensor updates are pushed by the camera every few minutes and on change.

## RTMP access control (deny-first)

Both publishing (camera → bridge) and play (viewer → bridge) are checked
against an IP allowlist **before** any stream is allocated. Empty
presets/IPs deny everything; malformed IPs/CIDRs or unknown presets fail
startup instead of falling open.

- `rtmp_allowed_presets` defaults to `hassio,frigate,localhost`
  (`172.30.32.0/23`, loopback). The `docker` and `private` presets remain
  explicit opt-ins — `private` also covers link-local/IPv6 ranges, so only
  enable it deliberately.
- A physical camera publishing locally is **not** covered by the defaults:
  add its exact observed peer IP to `rtmp_allowed_ips`
  (e.g. `192.168.1.50`). There is no `camera` preset that guesses your LAN.
- An external Frigate (outside the HA add-on network) likewise needs its
  actual peer IP in `rtmp_allowed_ips`. The bundled Frigate add-on on the
  standard HA network works under the defaults.
- Never expose the RTMP port to the internet; the ACL is convenience
  allowlisting, not authentication or encryption.

## Frigate (config-level smoke, no live decoding verified)

Point a Frigate camera at the bridge's local RTMP path and let Frigate pull
as a viewer:

```yaml
cameras:
  nanit_nursery:
    ffmpeg:
      inputs:
        - path: rtmp://<home-assistant-ip>:1935/local/<baby_uid>
          input_args: preset-rtmp-generic
          roles:
            - detect
```

- Always use the `/local/<baby_uid>` path, never `/remote`.
- Size `fps`/dimensions to your own camera; see `docs/frigate-example.yaml`
  for a minimal starting point with placeholder host/UID.
- If Frigate sees no frames, first check the bridge log for denied publish
  attempts and add the camera's peer IP to `rtmp_allowed_ips` (producer
  prerequisite above). Consumer-side, an external Frigate needs its own IP
  allowlisted too.
