package rtmpserver

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

// TestRTMPACL verifies RTMP IP ACL enforcement for both publish and play
// paths (shared handleConnectionWithSecurity gate): defaults permit the
// documented HA consumer topology, deny unknown peers before any
// broadcaster registration, explicit camera IPs are permitted, and malformed
// rules fail startup without falling open.
func TestRTMPACL(t *testing.T) {
	newMgr := func() *baby.StateManager { return baby.NewStateManager() }

	t.Run("defaults permit HA consumer and loopback", func(t *testing.T) {
		srv, err := NewServer(ServerConfig{
			ListenAddr:     "127.0.0.1:0",
			AllowedPresets: "hassio,frigate,localhost",
		}, newMgr())
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		if srv.ipAccess == nil {
			t.Fatal("ipAccess should be configured")
		}
		for _, addr := range []string{"127.0.0.1:5000", "172.30.32.10:1935", "[::1]:1935"} {
			if !srv.ipAccess.IsAllowed(addr) {
				t.Errorf("IsAllowed(%q) = false, want true", addr)
			}
		}
	})

	t.Run("defaults deny external peers before registration", func(t *testing.T) {
		srv, err := NewServer(ServerConfig{
			ListenAddr:     "127.0.0.1:0",
			AllowedPresets: "hassio,frigate,localhost",
		}, newMgr())
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		// No broadcaster exists yet; a denied peer must not create one.
		if got := srv.GetBroadcaster("baby1"); got != nil {
			t.Fatalf("unexpected broadcaster before any connection")
		}
		for _, addr := range []string{"192.168.1.50:1935", "10.1.2.3:1935", "8.8.8.8:1935"} {
			if srv.ipAccess.IsAllowed(addr) {
				t.Errorf("IsAllowed(%q) = true, want false (deny before registration)", addr)
			}
		}
		if got := srv.GetBroadcaster("baby1"); got != nil {
			t.Error("denied peer must not register a broadcaster")
		}
	})

	t.Run("explicit camera IP permitted for publish", func(t *testing.T) {
		srv, err := NewServer(ServerConfig{
			ListenAddr:     "127.0.0.1:0",
			AllowedPresets: "hassio,frigate,localhost",
			AllowedIPs:     "192.168.1.50",
		}, newMgr())
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		if !srv.ipAccess.IsAllowed("192.168.1.50:1935") {
			t.Error("explicit camera IP should be allowed to publish")
		}
		// Play path shares the same gate: HA consumer still allowed.
		if !srv.ipAccess.IsAllowed("172.30.32.10:1935") {
			t.Error("HA consumer should remain allowed for play")
		}
	})

	t.Run("malformed rules fail startup", func(t *testing.T) {
		for _, cfg := range []ServerConfig{
			{ListenAddr: "127.0.0.1:0", AllowedPresets: "nope"},
			{ListenAddr: "127.0.0.1:0", AllowedPresets: "hassio", AllowedIPs: "bogus"},
			{ListenAddr: "127.0.0.1:0", AllowedPresets: "hassio", AllowedIPs: "10.0.0.0/99"},
		} {
			if _, err := NewServer(cfg, newMgr()); err == nil {
				t.Errorf("NewServer(%+v) = nil error, want fail-closed", cfg)
			}
		}
	})
}
