#!/usr/bin/with-contenv bashio
# shellcheck shell=bash
set -e

if ! bashio::services.available "mqtt"; then
  bashio::log.warning "No MQTT service available - install & start the Mosquitto add-on."
fi

export NANIT_LOG_LEVEL="$(bashio::config 'log_level')"
export NANIT_SESSION_FILE="/data/session.json"

if bashio::config.has_value 'nanit_refresh_token'; then
  export NANIT_REFRESH_TOKEN="$(bashio::config 'nanit_refresh_token')"
elif ! bashio::fs.file_exists '/data/session.json'; then
  bashio::exit.nok "No nanit_refresh_token set and no saved session - run get-token.sh and paste the token into the add-on config."
fi

# RTMP camera feed
if bashio::config.has_value 'rtmp_addr'; then
  export NANIT_RTMP_ENABLED="true"
  export NANIT_RTMP_ADDR="$(bashio::config 'rtmp_addr')"
else
  bashio::log.warning "rtmp_addr not set - camera feed disabled, sensors only."
  export NANIT_RTMP_ENABLED="false"
fi

# RTMP IP access control (deny-first; explicit camera peer required)
export NANIT_RTMP_ALLOWED_PRESETS="$(bashio::config 'rtmp_allowed_presets')"
export NANIT_RTMP_ALLOWED_IPS="$(bashio::config 'rtmp_allowed_ips')"

# MQTT - wired automatically to the Mosquitto add-on
export NANIT_MQTT_ENABLED="true"
export NANIT_MQTT_BROKER_URL="mqtt://$(bashio::services mqtt 'host'):$(bashio::services mqtt 'port')"
export NANIT_MQTT_USERNAME="$(bashio::services mqtt 'username')"
export NANIT_MQTT_PASSWORD="$(bashio::services mqtt 'password')"
export NANIT_MQTT_PREFIX="nanit"
export NANIT_MQTT_DISCOVERY="$(bashio::config 'mqtt_discovery')"

# Single message polling loop (websocket alone is not enough for motion/sound)
export NANIT_EVENTS_POLLING="$(bashio::config 'event_polling')"

# Prepare only the bridge-owned data paths; never recursively chown mounts.
mkdir -p /data/video /data/log
session_dir="$(dirname "${NANIT_SESSION_FILE}")"
mkdir -p "${session_dir}"
chown nanit:nanit /data/video /data/log "${session_dir}"
if [ -e "${NANIT_SESSION_FILE}" ]; then
  chown nanit:nanit "${NANIT_SESSION_FILE}"
fi
# Writability must be proven as the runtime user, not root: root's -w
# passes on a 0500 owner directory that nanit (1000:1000) cannot write.
if ! gosu nanit:nanit test -w /data/video || ! gosu nanit:nanit test -w /data/log || ! gosu nanit:nanit test -w "${session_dir}"; then
  bashio::exit.nok "Data directory not writable by nanit (1000:1000) - check volume ownership for /data/video, /data/log and ${session_dir}."
fi

bashio::log.info "Nanit bridge starting (RTMP='${NANIT_RTMP_ADDR:-off}', MQTT discovery=${NANIT_MQTT_DISCOVERY})"
exec gosu nanit:nanit /usr/bin/nanit
