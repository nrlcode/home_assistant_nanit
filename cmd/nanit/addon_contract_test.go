package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// file is .../nanit/cmd/nanit/addon_contract_test.go -> root is ../../
	root := filepath.Join(filepath.Dir(file), "..", "..")
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("Abs error = %v", err)
	}
	return abs
}

func readYAML(t *testing.T, path string, out interface{}) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		t.Fatalf("yaml unmarshal %q: %v", path, err)
	}
}

func validateSample(schema map[string]string, sample map[string]interface{}) error {
	for key, typ := range schema {
		val, ok := sample[key]
		if !ok {
			if strings.HasSuffix(typ, "?") {
				continue
			}
			return fmt.Errorf("missing %q", key)
		}
		switch {
		case typ == "bool":
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("%q must be bool, got %T", key, val)
			}
		case typ == "password" || typ == "str" || typ == "str?":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("%q must be string, got %T", key, val)
			}
		case len(typ) > 5 && typ[:5] == "list(":
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("%q must be string list option, got %T", key, val)
			}
			inner := typ[5 : len(typ)-1]
			allowed := strings.Split(inner, "|")
			found := false
			for _, a := range allowed {
				if s == a {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("%q=%q not in %v", key, s, allowed)
			}
		default:
			return fmt.Errorf("unknown schema type %q for %q", typ, key)
		}
	}
	return nil
}

// TestAddonContract validates the merged add-on layout, schema parity, env
// mapping, and local-source Docker build contract.
func TestAddonContract(t *testing.T) {
	root := moduleRoot(t)

	t.Run("layout", func(t *testing.T) {
		for _, rel := range []string{
			"go.mod",
			"cmd/nanit/main.go",
			"pkg/mqtt/discovery.go",
			"pkg/mqtt/mqtt.go",
			"pkg/mqtt/opts.go",
			"pkg/security/ipaccess/ipaccess.go",
			"pkg/security/ipaccess/presets.go",
			"pkg/stream/hybrid_pool.go",
			"pkg/stream/manager.go",
			"pkg/rtmpserver/server.go",
			"pkg/rtmpserver/persistent_broadcaster.go",
			"pkg/notification/manager.go",
			"pkg/notification/poller.go",
			"config.yaml",
			"Dockerfile",
			"rootfs/run.sh",
			"docs/frigate-example.yaml",
			"examples/home-assistant-sleep-dashboard.yaml",
			"examples/home-assistant-sleep-recorder.yaml",
		} {
			if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
				t.Errorf("missing %q: %v", rel, err)
			}
		}
		// Must not import credential helper or nested source copies.
		for _, rel := range []string{
			"nanit/get-token.sh",
			"get-token.sh",
		} {
			if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
				t.Errorf("excluded file %q must not exist in module", rel)
			}
		}
		// Sleep polling is integrated into the existing notification lifecycle; no independent tracker is allowed.
		for _, rel := range []string{
			"pkg/notification/sleep_event.go",
			"pkg/notification/stats.go",
			"pkg/notification/sleep_manager.go",
		} {
			if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
				t.Errorf("sleep subsystem %q missing: %v", rel, err)
			}
		}
		if _, err := os.Stat(filepath.Join(root, "pkg/notification/sleep_state_tracker.go")); err == nil {
			t.Error("independent sleep state tracker must remain excluded")
		}
	})

	t.Run("options schema parity", func(t *testing.T) {
		var cfg struct {
			Options map[string]interface{} `yaml:"options"`
			Schema  map[string]interface{} `yaml:"schema"`
			Arch    []string               `yaml:"arch"`
		}
		readYAML(t, filepath.Join(root, "config.yaml"), &cfg)
		for _, key := range []string{
			"nanit_refresh_token", "rtmp_allowed_presets",
			"mqtt_discovery", "event_polling", "log_level",
		} {
			if _, ok := cfg.Options[key]; !ok {
				t.Errorf("options missing %q", key)
			}
			if _, ok := cfg.Schema[key]; !ok {
				t.Errorf("schema missing %q", key)
			}
		}
		if cfg.Schema["nanit_refresh_token"] != "password" {
			t.Errorf("nanit_refresh_token schema = %v, want password", cfg.Schema["nanit_refresh_token"])
		}
		// No sleep/notification tuning knobs.
		for k := range cfg.Options {
			if strings.Contains(k, "sleep") || strings.Contains(k, "notification") {
				t.Errorf("unexpected tuning knob %q", k)
			}
		}
	})

	t.Run("safe defaults", func(t *testing.T) {
		var cfg struct {
			Options map[string]interface{} `yaml:"options"`
		}
		readYAML(t, filepath.Join(root, "config.yaml"), &cfg)
		if got := cfg.Options["rtmp_allowed_presets"]; got != "hassio,frigate,localhost" {
			t.Errorf("rtmp_allowed_presets default = %v, want hassio,frigate,localhost", got)
		}
		if got := cfg.Options["rtmp_allowed_ips"]; got != nil {
			t.Errorf("rtmp_allowed_ips default = %v, want unset", got)
		}
	})

	t.Run("docker local source", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
		if err != nil {
			t.Fatalf("Dockerfile read: %v", err)
		}
		docker := string(data)
		if !strings.Contains(docker, "golang:1.24.11") {
			t.Error("Dockerfile must build with Go 1.24.11")
		}
		if !strings.Contains(docker, "COPY") {
			t.Error("Dockerfile must COPY local source")
		}
		for _, bad := range []string{"git clone", "NANIT_REF"} {
			if strings.Contains(docker, bad) {
				t.Errorf("Dockerfile must not contain %q (no remote fetch)", bad)
			}
		}
		if !strings.Contains(docker, "gosu") {
			t.Error("Dockerfile must install privilege-drop utility (gosu)")
		}
	})

	t.Run("runtime env mapping and non-root", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(root, "rootfs", "run.sh"))
		if err != nil {
			t.Fatalf("run.sh read: %v", err)
		}
		sh := string(data)
		for _, want := range []string{
			"NANIT_RTMP_ALLOWED_PRESETS",
			"NANIT_RTMP_ALLOWED_IPS",
			"rtmp_allowed_presets",
			"rtmp_allowed_ips",
			"NANIT_MQTT_DISCOVERY",
			"NANIT_EVENTS_POLLING",
			"NANIT_RTMP_ADDR",
			"gosu nanit",
			"/data/video",
			"/data/log",
		} {
			if !strings.Contains(sh, want) {
				t.Errorf("run.sh missing %q", want)
			}
		}
		if strings.Contains(sh, "NANIT_SLEEP") || strings.Contains(sh, "NANIT_NOTIFICATIONS") {
			t.Error("run.sh must not contain sleep polling envs")
		}
		if strings.Contains(sh, "chown -R") || strings.Contains(sh, "chmod -R") {
			t.Error("run.sh must not recursively chown arbitrary mounts")
		}
		if !strings.Contains(sh, "exec gosu nanit") {
			t.Error("run.sh must exec bridge as nanit (non-root)")
		}
		// Writability must be proven as nanit via the privilege-drop
		// utility, not root's -w (which passes on 0500 owner dirs).
		if !strings.Contains(sh, "gosu nanit:nanit test -w") {
			t.Error("run.sh must check writability via 'gosu nanit:nanit test -w'")
		}
		if strings.Contains(sh, "[ ! -w /data/video ]") || strings.Contains(sh, "[ ! -w /data/log ]") {
			t.Error("run.sh must not use root's '[ ! -w ... ]' for runtime paths")
		}
		if !strings.Contains(sh, "not writable by nanit (1000:1000)") {
			t.Error("run.sh must fail with the actionable nanit ownership diagnostic")
		}
	})

	t.Run("writability synthetic paths", func(t *testing.T) {
		// Execute the writability semantics: 0500 owner dir is not
		// writable, 0700 is. We run as UID 1000 (same numeric as nanit),
		// so `test -w` here mirrors `gosu nanit:nanit test -w`.
		writable := t.TempDir()
		blocked := filepath.Join(t.TempDir(), "blocked")
		if err := os.Mkdir(blocked, 0o500); err != nil {
			t.Fatalf("mkdir blocked: %v", err)
		}
		defer func() { _ = os.Chmod(blocked, 0o700) }()
		if _, err := os.Stat(writable); err != nil {
			t.Fatalf("stat writable: %v", err)
		}
		// Writable path must pass.
		if f, err := os.OpenFile(filepath.Join(writable, "probe"), os.O_CREATE|os.O_WRONLY, 0o600); err != nil {
			t.Errorf("writable synthetic path not writable: %v", err)
		} else {
			_ = f.Close()
		}
		// Inaccessible synthetic path must fail.
		if f, err := os.OpenFile(filepath.Join(blocked, "probe"), os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			_ = f.Close()
			t.Error("0500 synthetic path accepted writes, want failure")
		}
	})

	t.Run("schema samples", func(t *testing.T) {
		var cfg struct {
			Schema map[string]string `yaml:"schema"`
		}
		readYAML(t, filepath.Join(root, "config.yaml"), &cfg)
		valid := map[string]interface{}{
			"nanit_refresh_token":  "secret",
			"rtmp_allowed_presets": "hassio,frigate,localhost",
			"mqtt_discovery":       true,
			"event_polling":        true,
			"log_level":            "info",
		}
		if err := validateSample(cfg.Schema, valid); err != nil {
			t.Errorf("valid sample rejected: %v", err)
		}
		invalid := []map[string]interface{}{
			{"nanit_refresh_token": "s", "rtmp_allowed_presets": "x", "mqtt_discovery": "yes", "event_polling": true, "log_level": "info"},
			{"nanit_refresh_token": "s", "rtmp_allowed_presets": "x", "mqtt_discovery": true, "event_polling": true, "log_level": "verbose"},
		}
		for i, sample := range invalid {
			if err := validateSample(cfg.Schema, sample); err == nil {
				t.Errorf("invalid sample %d accepted, want rejection", i)
			}
		}
	})

	t.Run("empty rtmp_addr sensors-only", func(t *testing.T) {
		data, _ := os.ReadFile(filepath.Join(root, "rootfs", "run.sh"))
		sh := string(data)
		if !strings.Contains(sh, "NANIT_RTMP_ENABLED=\"false\"") {
			t.Error("run.sh must set RTMP_ENABLED=false when rtmp_addr empty (sensors-only)")
		}
	})
}

// TestFrigateConfig validates the Frigate example input (config/doc smoke,
// not live decoding): expected URL shape, roles, preset, no /remote path, and
// ACL producer documentation.
func TestFrigateConfig(t *testing.T) {
	root := moduleRoot(t)

	var frigate struct {
		Cameras map[string]struct {
			FFmpeg struct {
				Inputs []struct {
					Path      string   `yaml:"path"`
					InputArgs string   `yaml:"input_args"`
					Roles     []string `yaml:"roles"`
				} `yaml:"inputs"`
			} `yaml:"ffmpeg"`
		} `yaml:"cameras"`
	}
	readYAML(t, filepath.Join(root, "docs", "frigate-example.yaml"), &frigate)
	if len(frigate.Cameras) == 0 {
		t.Fatal("no cameras in frigate example")
	}
	for name, cam := range frigate.Cameras {
		if len(cam.FFmpeg.Inputs) == 0 {
			t.Errorf("camera %q has no inputs", name)
			continue
		}
		in := cam.FFmpeg.Inputs[0]
		if !strings.HasPrefix(in.Path, "rtmp://") || !strings.Contains(in.Path, "/local/") {
			t.Errorf("camera %q path = %q, want rtmp://<host>:1935/local/<baby_uid>", name, in.Path)
		}
		if strings.Contains(in.Path, "/remote") {
			t.Errorf("camera %q path must not use /remote", name)
		}
		hasDetect := false
		for _, r := range in.Roles {
			if r == "detect" {
				hasDetect = true
			}
		}
		if !hasDetect {
			t.Errorf("camera %q roles = %v, want detect", name, in.Roles)
		}
		if in.InputArgs != "preset-rtmp-generic" {
			t.Errorf("camera %q input_args = %q, want preset-rtmp-generic", name, in.InputArgs)
		}
	}

	// DOCS must carry producer ACL instructions alongside the Frigate path.
	docsData, err := os.ReadFile(filepath.Join(root, "DOCS.md"))
	if err != nil {
		t.Fatalf("DOCS.md read: %v", err)
	}
	docs := string(docsData)
	for _, want := range []string{"rtmp://", "/local/", "rtmp_allowed_ips", "preset-rtmp-generic"} {
		if !strings.Contains(docs, want) {
			t.Errorf("DOCS.md missing %q", want)
		}
	}
}
