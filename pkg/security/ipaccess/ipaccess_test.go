package ipaccess

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"
)

func TestController_IsAllowed(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name           string
		allowedIPs     string
		allowedPresets string
		testAddr       string
		want           bool
	}{
		// Explicit IP tests
		{
			name:       "explicit IP match",
			allowedIPs: "192.168.1.100",
			testAddr:   "192.168.1.100",
			want:       true,
		},
		{
			name:       "explicit IP match with port",
			allowedIPs: "192.168.1.100",
			testAddr:   "192.168.1.100:54321",
			want:       true,
		},
		{
			name:       "explicit IP no match",
			allowedIPs: "192.168.1.100",
			testAddr:   "192.168.1.101",
			want:       false,
		},
		{
			name:       "multiple explicit IPs match second",
			allowedIPs: "192.168.1.100,192.168.1.101,192.168.1.102",
			testAddr:   "192.168.1.101",
			want:       true,
		},

		// CIDR tests
		{
			name:       "CIDR /24 match",
			allowedIPs: "192.168.1.0/24",
			testAddr:   "192.168.1.50",
			want:       true,
		},
		{
			name:       "CIDR /24 no match",
			allowedIPs: "192.168.1.0/24",
			testAddr:   "192.168.2.50",
			want:       false,
		},
		{
			name:       "CIDR /16 match",
			allowedIPs: "172.30.0.0/16",
			testAddr:   "172.30.32.5",
			want:       true,
		},
		{
			name:       "CIDR /8 match",
			allowedIPs: "10.0.0.0/8",
			testAddr:   "10.255.255.255",
			want:       true,
		},

		// Preset tests
		{
			name:           "localhost preset match",
			allowedPresets: "localhost",
			testAddr:       "127.0.0.1",
			want:           true,
		},
		{
			name:           "localhost preset match any loopback",
			allowedPresets: "localhost",
			testAddr:       "127.0.0.55",
			want:           true,
		},
		{
			name:           "localhost preset no match",
			allowedPresets: "localhost",
			testAddr:       "192.168.1.1",
			want:           false,
		},
		{
			name:           "hassio preset match addon network",
			allowedPresets: "hassio",
			testAddr:       "172.30.32.5",
			want:           true,
		},
		{
			name:           "hassio preset match localhost",
			allowedPresets: "hassio",
			testAddr:       "127.0.0.1",
			want:           true,
		},
		{
			name:           "hassio preset no match external",
			allowedPresets: "hassio",
			testAddr:       "192.168.1.100",
			want:           false,
		},
		{
			name:           "private preset match 10.x",
			allowedPresets: "private",
			testAddr:       "10.0.0.1",
			want:           true,
		},
		{
			name:           "private preset match 192.168.x",
			allowedPresets: "private",
			testAddr:       "192.168.100.50",
			want:           true,
		},
		{
			name:           "private preset no match public",
			allowedPresets: "private",
			testAddr:       "8.8.8.8",
			want:           false,
		},

		// Combined tests
		{
			name:           "preset and explicit IP combined",
			allowedPresets: "localhost",
			allowedIPs:     "192.168.1.100",
			testAddr:       "192.168.1.100",
			want:           true,
		},
		{
			name:           "multiple presets",
			allowedPresets: "localhost,hassio",
			testAddr:       "172.30.32.10",
			want:           true,
		},

		// No rules configured
		{
			name:     "no rules denies all",
			testAddr: "192.168.1.1",
			want:     false,
		},

		// Edge cases
		{
			name:       "whitespace in config",
			allowedIPs: "  192.168.1.100  ,  192.168.1.101  ",
			testAddr:   "192.168.1.100",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(Config{
				AllowedIPs:     tt.allowedIPs,
				AllowedPresets: tt.allowedPresets,
				Logger:         logger,
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			if got := c.IsAllowed(tt.testAddr); got != tt.want {
				t.Errorf("IsAllowed(%q) = %v, want %v", tt.testAddr, got, tt.want)
			}
		})
	}
}

func TestController_InvalidConfig(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name           string
		allowedIPs     string
		allowedPresets string
		wantErr        bool
	}{
		{
			name:       "invalid IP",
			allowedIPs: "not-an-ip",
			wantErr:    true,
		},
		{
			name:       "invalid CIDR",
			allowedIPs: "192.168.1.0/99",
			wantErr:    true,
		},
		{
			name:           "invalid preset",
			allowedPresets: "not-a-preset",
			wantErr:        true,
		},
		{
			name:       "valid config",
			allowedIPs: "192.168.1.0/24,10.0.0.1",
			wantErr:    false,
		},
		{
			name:           "valid preset",
			allowedPresets: "hassio",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				AllowedIPs:     tt.allowedIPs,
				AllowedPresets: tt.allowedPresets,
				Logger:         logger,
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestController_DynamicAdd(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedIPs: "192.168.1.1",
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Initially not allowed
	if c.IsAllowed("10.0.0.1") {
		t.Error("10.0.0.1 should not be allowed initially")
	}

	// Add dynamically
	if err := c.AddCIDR("10.0.0.0/8"); err != nil {
		t.Fatalf("AddCIDR() error = %v", err)
	}

	// Now should be allowed
	if !c.IsAllowed("10.0.0.1") {
		t.Error("10.0.0.1 should be allowed after AddCIDR")
	}

	// Add specific IP
	if err := c.AddIP("8.8.8.8"); err != nil {
		t.Fatalf("AddIP() error = %v", err)
	}

	if !c.IsAllowed("8.8.8.8") {
		t.Error("8.8.8.8 should be allowed after AddIP")
	}
}

func TestController_Stats(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedIPs:     "192.168.1.1,192.168.1.2",
		AllowedPresets: "localhost,hassio",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ipCount, cidrCount, presets := c.Stats()

	if ipCount != 2 {
		t.Errorf("ipCount = %d, want 2", ipCount)
	}

	// localhost has 2 CIDRs, hassio has 3 CIDRs = 5 total
	if cidrCount < 4 {
		t.Errorf("cidrCount = %d, want >= 4", cidrCount)
	}

	if len(presets) != 2 {
		t.Errorf("len(presets) = %d, want 2", len(presets))
	}
}

func TestController_IsEnabled(t *testing.T) {
	logger := zerolog.Nop()

	// Empty config
	c1, _ := New(Config{Logger: logger})
	if c1.IsEnabled() {
		t.Error("empty config should not be enabled")
	}

	// With IPs
	c2, _ := New(Config{AllowedIPs: "192.168.1.1", Logger: logger})
	if !c2.IsEnabled() {
		t.Error("config with IPs should be enabled")
	}

	// With presets
	c3, _ := New(Config{AllowedPresets: "localhost", Logger: logger})
	if !c3.IsEnabled() {
		t.Error("config with presets should be enabled")
	}
}

func TestIPv6(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedPresets: "localhost",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// IPv6 loopback should be allowed
	if !c.IsAllowed("::1") {
		t.Error("::1 should be allowed with localhost preset")
	}

	// IPv6 with port format
	if !c.IsAllowed("[::1]:8080") {
		t.Error("[::1]:8080 should be allowed with localhost preset")
	}
}

func TestCIDRBoundaries(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedIPs: "192.168.1.0/24",
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// First IP in range
	if !c.IsAllowed("192.168.1.0") {
		t.Error("192.168.1.0 should be allowed (first in /24)")
	}

	// Last IP in range
	if !c.IsAllowed("192.168.1.255") {
		t.Error("192.168.1.255 should be allowed (last in /24)")
	}

	// Just outside range
	if c.IsAllowed("192.168.0.255") {
		t.Error("192.168.0.255 should NOT be allowed (before /24)")
	}
	if c.IsAllowed("192.168.2.0") {
		t.Error("192.168.2.0 should NOT be allowed (after /24)")
	}
}

func TestSingleIPAsCIDR(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedIPs: "192.168.1.100/32",
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Exact match
	if !c.IsAllowed("192.168.1.100") {
		t.Error("192.168.1.100 should be allowed with /32 CIDR")
	}

	// Adjacent IP should not match
	if c.IsAllowed("192.168.1.101") {
		t.Error("192.168.1.101 should NOT be allowed with /32 CIDR")
	}
}

func TestEmptyAndInvalidInputs(t *testing.T) {
	logger := zerolog.Nop()

	// Empty config should create successfully
	c, err := New(Config{
		AllowedIPs:     "",
		AllowedPresets: "",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("Empty config should not error: %v", err)
	}

	// But should deny everything
	if c.IsAllowed("192.168.1.1") {
		t.Error("empty config should deny all IPs")
	}

	// Invalid address format should be denied
	if c.IsAllowed("not-an-ip") {
		t.Error("invalid IP format should be denied")
	}

	if c.IsAllowed("") {
		t.Error("empty address should be denied")
	}

	if c.IsAllowed("192.168.1") {
		t.Error("incomplete IP should be denied")
	}
}

func TestPresetCaseInsensitivity(t *testing.T) {
	logger := zerolog.Nop()

	// Uppercase
	c1, err := New(Config{
		AllowedPresets: "HASSIO",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("Uppercase preset should work: %v", err)
	}
	if !c1.IsAllowed("172.30.32.5") {
		t.Error("HASSIO preset should work same as hassio")
	}

	// Mixed case
	c2, err := New(Config{
		AllowedPresets: "HaSSio",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("Mixed case preset should work: %v", err)
	}
	if !c2.IsAllowed("172.30.32.5") {
		t.Error("HaSSio preset should work same as hassio")
	}
}

func TestAllPresets(t *testing.T) {
	logger := zerolog.Nop()

	presetTests := []struct {
		preset    string
		allowedIP string
		deniedIP  string
	}{
		{"localhost", "127.0.0.1", "192.168.1.1"},
		{"hassio", "172.30.32.5", "192.168.1.1"},
		{"frigate", "172.30.32.5", "192.168.1.1"},
		{"docker", "172.17.0.1", "192.168.1.1"},
		{"private", "192.168.1.1", "8.8.8.8"},
	}

	for _, tt := range presetTests {
		t.Run(tt.preset, func(t *testing.T) {
			c, err := New(Config{
				AllowedPresets: tt.preset,
				Logger:         logger,
			})
			if err != nil {
				t.Fatalf("preset %s should be valid: %v", tt.preset, err)
			}

			if !c.IsAllowed(tt.allowedIP) {
				t.Errorf("preset %s should allow %s", tt.preset, tt.allowedIP)
			}

			if c.IsAllowed(tt.deniedIP) {
				t.Errorf("preset %s should deny %s", tt.preset, tt.deniedIP)
			}
		})
	}
}

func TestPrivatePresetRanges(t *testing.T) {
	logger := zerolog.Nop()

	c, err := New(Config{
		AllowedPresets: "private",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// 10.0.0.0/8 range
	if !c.IsAllowed("10.0.0.1") {
		t.Error("10.0.0.1 should be allowed")
	}
	if !c.IsAllowed("10.255.255.255") {
		t.Error("10.255.255.255 should be allowed")
	}

	// 172.16.0.0/12 range (172.16.x.x - 172.31.x.x)
	if !c.IsAllowed("172.16.0.1") {
		t.Error("172.16.0.1 should be allowed")
	}
	if !c.IsAllowed("172.31.255.255") {
		t.Error("172.31.255.255 should be allowed")
	}
	if c.IsAllowed("172.15.255.255") {
		t.Error("172.15.255.255 should NOT be allowed (outside /12)")
	}
	if c.IsAllowed("172.32.0.0") {
		t.Error("172.32.0.0 should NOT be allowed (outside /12)")
	}

	// 192.168.0.0/16 range
	if !c.IsAllowed("192.168.0.1") {
		t.Error("192.168.0.1 should be allowed")
	}
	if !c.IsAllowed("192.168.255.255") {
		t.Error("192.168.255.255 should be allowed")
	}

	// Link-local
	if !c.IsAllowed("169.254.1.1") {
		t.Error("169.254.1.1 should be allowed (link-local)")
	}

	// Public IPs should be denied
	if c.IsAllowed("8.8.8.8") {
		t.Error("8.8.8.8 should NOT be allowed")
	}
	if c.IsAllowed("1.1.1.1") {
		t.Error("1.1.1.1 should NOT be allowed")
	}
}

func TestDynamicAddErrors(t *testing.T) {
	logger := zerolog.Nop()

	c, _ := New(Config{
		AllowedIPs: "192.168.1.1",
		Logger:     logger,
	})

	// Invalid IP
	if err := c.AddIP("not-an-ip"); err == nil {
		t.Error("AddIP with invalid IP should return error")
	}

	// Invalid CIDR
	if err := c.AddCIDR("192.168.1.0/99"); err == nil {
		t.Error("AddCIDR with invalid CIDR should return error")
	}

	// Not a CIDR
	if err := c.AddCIDR("192.168.1.1"); err == nil {
		t.Error("AddCIDR with plain IP should return error")
	}
}

func TestStringOutput(t *testing.T) {
	logger := zerolog.Nop()

	// Empty config
	c1, _ := New(Config{Logger: logger})
	str1 := c1.String()
	if str1 == "" {
		t.Error("String() should return non-empty for empty config")
	}

	// With presets
	c2, _ := New(Config{AllowedPresets: "hassio", Logger: logger})
	str2 := c2.String()
	if str2 == "" {
		t.Error("String() should return non-empty for preset config")
	}
	if !contains(str2, "hassio") {
		t.Errorf("String() should mention preset name, got: %s", str2)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestConcurrentAccess(t *testing.T) {
	logger := zerolog.Nop()

	c, _ := New(Config{
		AllowedIPs: "192.168.1.0/24",
		Logger:     logger,
	})

	// Run multiple goroutines checking IPs concurrently
	done := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				c.IsAllowed("192.168.1.50")
				c.IsAllowed("10.0.0.1")
			}
			done <- true
		}(i)
	}

	// Also add IPs concurrently
	for i := 0; i < 10; i++ {
		go func(id int) {
			c.AddIP("10.0.0.1")
			c.AddCIDR("10.0.0.0/8")
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 110; i++ {
		<-done
	}
}

func TestRealWorldScenario(t *testing.T) {
	logger := zerolog.Nop()

	// Simulate: Camera on 192.168.1.50, Frigate on Docker hassio network
	c, err := New(Config{
		AllowedPresets: "hassio",
		AllowedIPs:     "192.168.1.50",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Camera should be allowed
	if !c.IsAllowed("192.168.1.50:54321") {
		t.Error("Camera IP should be allowed")
	}

	// Frigate container should be allowed
	if !c.IsAllowed("172.30.32.5:45678") {
		t.Error("Frigate container should be allowed")
	}

	// Local testing should work
	if !c.IsAllowed("127.0.0.1:1935") {
		t.Error("Localhost should be allowed")
	}

	// Random IoT device should be blocked
	if c.IsAllowed("192.168.1.100:12345") {
		t.Error("Random IoT device should be blocked")
	}

	// Internet IP should be blocked
	if c.IsAllowed("203.0.113.50:80") {
		t.Error("Internet IP should be blocked")
	}
}

func TestIPDeduplication(t *testing.T) {
	logger := zerolog.Nop()

	c, _ := New(Config{
		AllowedIPs: "192.168.1.1",
		Logger:     logger,
	})

	// Get initial count
	ipCount1, _, _ := c.Stats()

	// Add same IP again
	if err := c.AddIP("192.168.1.1"); err != nil {
		t.Fatalf("AddIP should not error on duplicate: %v", err)
	}

	// Count should not increase
	ipCount2, _, _ := c.Stats()
	if ipCount2 != ipCount1 {
		t.Errorf("IP count increased after duplicate add: %d -> %d", ipCount1, ipCount2)
	}

	// Add different IP
	if err := c.AddIP("192.168.1.2"); err != nil {
		t.Fatalf("AddIP error: %v", err)
	}

	// Count should increase by 1
	ipCount3, _, _ := c.Stats()
	if ipCount3 != ipCount1+1 {
		t.Errorf("IP count should be %d, got %d", ipCount1+1, ipCount3)
	}
}

func TestCIDRDeduplication(t *testing.T) {
	logger := zerolog.Nop()

	c, _ := New(Config{
		AllowedIPs: "192.168.1.0/24",
		Logger:     logger,
	})

	// Get initial count
	_, cidrCount1, _ := c.Stats()

	// Add same CIDR again
	if err := c.AddCIDR("192.168.1.0/24"); err != nil {
		t.Fatalf("AddCIDR should not error on duplicate: %v", err)
	}

	// Count should not increase
	_, cidrCount2, _ := c.Stats()
	if cidrCount2 != cidrCount1 {
		t.Errorf("CIDR count increased after duplicate add: %d -> %d", cidrCount1, cidrCount2)
	}

	// Add different CIDR
	if err := c.AddCIDR("10.0.0.0/8"); err != nil {
		t.Fatalf("AddCIDR error: %v", err)
	}

	// Count should increase by 1
	_, cidrCount3, _ := c.Stats()
	if cidrCount3 != cidrCount1+1 {
		t.Errorf("CIDR count should be %d, got %d", cidrCount1+1, cidrCount3)
	}

	// Add equivalent CIDR with different notation (should still dedupe)
	// 192.168.1.0/24 vs 192.168.1.128/24 - these are different
	// But 192.168.1.0/24 added again should be caught
	if err := c.AddCIDR("192.168.1.0/24"); err != nil {
		t.Fatalf("AddCIDR should not error: %v", err)
	}
	_, cidrCount4, _ := c.Stats()
	if cidrCount4 != cidrCount3 {
		t.Errorf("CIDR count should not change for duplicate: %d -> %d", cidrCount3, cidrCount4)
	}
}

// Benchmarks

func BenchmarkIsAllowed_SingleIP(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedIPs: "192.168.1.100",
		Logger:     logger,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IsAllowed("192.168.1.100:54321")
	}
}

func BenchmarkIsAllowed_SingleCIDR(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedIPs: "192.168.1.0/24",
		Logger:     logger,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IsAllowed("192.168.1.100:54321")
	}
}

func BenchmarkIsAllowed_MultipleCIDRs(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedPresets: "private", // Has ~8 CIDR ranges
		Logger:         logger,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IsAllowed("192.168.1.100:54321")
	}
}

func BenchmarkIsAllowed_Denied(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedPresets: "hassio", // Only allows hassio network + localhost
		Logger:         logger,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IsAllowed("8.8.8.8:53") // Will be denied - must check all rules
	}
}

func BenchmarkIsAllowed_ManyRanges(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedPresets: "docker", // Has ~15 CIDR ranges
		Logger:         logger,
	})

	// Add more ranges to stress test
	for i := 0; i < 100; i++ {
		c.AddCIDR(fmt.Sprintf("10.%d.0.0/16", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.IsAllowed("192.168.1.100:54321")
	}
}

func BenchmarkIsAllowed_Parallel(b *testing.B) {
	logger := zerolog.Nop()
	c, _ := New(Config{
		AllowedPresets: "private",
		Logger:         logger,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.IsAllowed("192.168.1.100:54321")
		}
	})
}
