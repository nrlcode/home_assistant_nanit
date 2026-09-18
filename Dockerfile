ARG BUILD_ARCH=amd64

# --- build the nanit binary from local source ---------------------------------
FROM golang:1.24.11-alpine AS build
ARG BUILD_ARCH
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY pkg ./pkg
RUN case "${BUILD_ARCH}" in \
      aarch64) export GOARCH=arm64 ;; \
      amd64) export GOARCH=amd64 ;; \
      armv7) export GOARCH=arm GOARM=7 ;; \
      *) echo "Unsupported BUILD_ARCH: ${BUILD_ARCH}" >&2; exit 1 ;; \
    esac \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /nanit ./cmd/nanit

# --- runtime ------------------------------------------------------------------
FROM ghcr.io/hassio-addons/base:18.2.1@sha256:149a3937e1d6daef610faceebd6eb6632b821c566a2c5084bb8f8c1e552899a5
ARG BUILD_ARCH
LABEL io.hass.version="1.0.0" \
      io.hass.type="app" \
      io.hass.arch="${BUILD_ARCH}" \
      org.opencontainers.image.title="Nanit Bridge" \
      org.opencontainers.image.source="https://github.com/WColan/home_assistant_nanit"
RUN apk add --no-cache ffmpeg gosu
COPY --from=build /nanit /usr/bin/nanit
COPY LICENSE /usr/share/licenses/nanit/LICENSE
COPY rootfs /
RUN chmod a+x /usr/bin/nanit /run.sh \
 && addgroup -g 1000 nanit 2>/dev/null || true \
 && adduser -D -u 1000 -G nanit nanit 2>/dev/null || true \
 && mkdir -p /data/video /data/log \
 && chown nanit:nanit /data/video /data/log
CMD [ "/run.sh" ]
