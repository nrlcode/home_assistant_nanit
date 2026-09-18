package ipaccess

import (
	"sort"
	"strings"
)

// Presets defines named IP range presets for common use cases.
// Each preset maps to a list of CIDR ranges.
var Presets = map[string][]string{
	// localhost - Only loopback addresses
	"localhost": {
		"127.0.0.0/8", // IPv4 loopback
		"::1/128",     // IPv6 loopback
	},

	// hassio - Home Assistant addon network ranges
	// The hassio network uses 172.30.32.0/23 by default
	// Supervisor API is on 172.30.32.2
	"hassio": {
		"172.30.32.0/23", // Primary HA addon network
		"172.30.33.0/24", // Extended addon network
		"127.0.0.0/8",    // Include localhost for local access
	},

	// docker - Common Docker network ranges
	// Docker uses 172.17.0.0/16 by default, but can use others
	"docker": {
		"172.17.0.0/16", // Default Docker bridge
		"172.18.0.0/16", // Additional Docker networks
		"172.19.0.0/16", // Additional Docker networks
		"172.20.0.0/16", // Additional Docker networks
		"172.21.0.0/16", // Additional Docker networks
		"172.22.0.0/16", // Additional Docker networks
		"172.23.0.0/16", // Additional Docker networks
		"172.24.0.0/16", // Additional Docker networks
		"172.25.0.0/16", // Additional Docker networks
		"172.26.0.0/16", // Additional Docker networks
		"172.27.0.0/16", // Additional Docker networks
		"172.28.0.0/16", // Additional Docker networks
		"172.29.0.0/16", // Additional Docker networks
		"172.30.0.0/16", // Additional Docker networks (includes hassio)
		"172.31.0.0/16", // Additional Docker networks
		"127.0.0.0/8",   // Include localhost
	},

	// private - All RFC1918 private address ranges
	// Use with caution - allows any device on private networks
	"private": {
		"10.0.0.0/8",     // Class A private
		"172.16.0.0/12",  // Class B private (includes Docker ranges)
		"192.168.0.0/16", // Class C private
		"127.0.0.0/8",    // Loopback
		"169.254.0.0/16", // Link-local
		"fc00::/7",       // IPv6 unique local
		"fe80::/10",      // IPv6 link-local
		"::1/128",        // IPv6 loopback
	},

	// frigate - Specifically for Frigate NVR addon
	// Same as hassio but documented separately for clarity
	"frigate": {
		"172.30.32.0/23", // HA addon network where Frigate runs
		"127.0.0.0/8",    // Localhost
	},

	// camera - Placeholder for camera IP (user should use explicit IP instead)
	// This is mainly for documentation purposes
	"camera": {
		// Users should add their camera's specific IP via NANIT_ALLOWED_IPS
		// Example: NANIT_ALLOWED_IPS=192.168.1.50
	},
}

// PresetDescriptions provides human-readable descriptions for each preset.
var PresetDescriptions = map[string]string{
	"localhost": "Loopback only (127.0.0.0/8, ::1)",
	"hassio":    "Home Assistant addon network (172.30.32.0/23)",
	"docker":    "Docker bridge networks (172.17-31.0.0/16)",
	"private":   "All RFC1918 private ranges (10.x, 172.16-31.x, 192.168.x)",
	"frigate":   "Frigate NVR addon (same as hassio)",
	"camera":    "Camera IP (use NANIT_ALLOWED_IPS instead)",
}

// validPresetNames returns a comma-separated list of valid preset names.
func validPresetNames() string {
	names := make([]string, 0, len(Presets))
	for name := range Presets {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// GetPresetDescription returns the description for a preset, or empty string if not found.
func GetPresetDescription(name string) string {
	return PresetDescriptions[strings.ToLower(name)]
}

// ListPresets returns all available preset names and their descriptions.
func ListPresets() map[string]string {
	result := make(map[string]string, len(PresetDescriptions))
	for k, v := range PresetDescriptions {
		result[k] = v
	}
	return result
}
