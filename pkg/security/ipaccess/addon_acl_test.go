package ipaccess

import (
	"testing"

	"github.com/rs/zerolog"
)

// TestAddonACLDefaults covers the merged add-on default ACL contract:
// presets hassio,frigate,localhost permit documented HA/loopback peers,
// deny unknown external peers, explicit single camera IP is permitted, empty
// core config denies all, and malformed rules fail closed at startup.
func TestAddonACLDefaults(t *testing.T) {
	logger := zerolog.Nop()

	newController := func(presets, ips string) *Controller {
		c, err := New(Config{
			AllowedPresets: presets,
			AllowedIPs:     ips,
			Logger:         logger,
		})
		if err != nil {
			t.Fatalf("New(%q,%q) error = %v", presets, ips, err)
		}
		return c
	}

	t.Run("default presets permit HA and loopback", func(t *testing.T) {
		c := newController("hassio,frigate,localhost", "")
		for _, addr := range []string{
			"127.0.0.1:1935",
			"127.0.0.2",
			"::1",
			"172.30.32.5:1935",
			"172.30.33.200",
		} {
			if !c.IsAllowed(addr) {
				t.Errorf("IsAllowed(%q) = false, want true under add-on defaults", addr)
			}
		}
	})

	t.Run("default presets deny external peers", func(t *testing.T) {
		c := newController("hassio,frigate,localhost", "")
		for _, addr := range []string{
			"192.168.1.50:1935",
			"10.0.0.5",
			"8.8.8.8",
			"172.17.0.5",
		} {
			if c.IsAllowed(addr) {
				t.Errorf("IsAllowed(%q) = true, want false under add-on defaults", addr)
			}
		}
	})

	t.Run("explicit single camera IP permitted alongside defaults", func(t *testing.T) {
		c := newController("hassio,frigate,localhost", "192.168.1.50")
		if !c.IsAllowed("192.168.1.50:1935") {
			t.Error("explicit camera IP should be allowed")
		}
		if c.IsAllowed("192.168.1.51") {
			t.Error("adjacent IP should still be denied")
		}
		// Defaults still hold.
		if !c.IsAllowed("127.0.0.1") {
			t.Error("loopback should remain allowed")
		}
	})

	t.Run("empty core ACL denies all", func(t *testing.T) {
		c := newController("", "")
		for _, addr := range []string{"127.0.0.1", "172.30.32.5", "192.168.1.50"} {
			if c.IsAllowed(addr) {
				t.Errorf("IsAllowed(%q) = true, want false with empty ACL", addr)
			}
		}
		if c.IsEnabled() {
			t.Error("IsEnabled() = true, want false with empty ACL")
		}
	})

	t.Run("malformed rules fail closed at startup", func(t *testing.T) {
		for _, cfg := range []Config{
			{AllowedPresets: "hassio,not-a-preset", Logger: logger},
			{AllowedPresets: "hassio", AllowedIPs: "999.999.1.1", Logger: logger},
			{AllowedPresets: "hassio", AllowedIPs: "10.0.0.0/99", Logger: logger},
		} {
			if _, err := New(cfg); err == nil {
				t.Errorf("New(%+v) = nil error, want fail-closed", cfg)
			}
		}
	})

	t.Run("all four requested presets resolve", func(t *testing.T) {
		for _, preset := range []string{"hassio", "frigate", "localhost", "docker"} {
			c := newController(preset, "")
			if !c.IsEnabled() {
				t.Errorf("preset %q should enable ACL", preset)
			}
		}
		// private remains an explicit opt-in (broad); it must resolve but
		// is not part of the default.
		c := newController("private", "")
		if !c.IsAllowed("192.168.7.7") {
			t.Error("private preset should allow RFC1918 peers when explicitly chosen")
		}
		def := newController("hassio,frigate,localhost", "")
		if def.IsAllowed("192.168.7.7") {
			t.Error("default presets must not imply private LAN access")
		}
	})
}
