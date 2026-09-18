// Package ipaccess provides IP-based access control for network services.
// It supports CIDR notation, named presets, and connection logging.
package ipaccess

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

// Controller manages IP-based access control with support for
// CIDR ranges, named presets, and connection attempt logging.
type Controller struct {
	mu             sync.RWMutex
	allowedNets    []*net.IPNet
	allowedIPs     []net.IP
	logDenied      bool
	logAllowed     bool
	logger         zerolog.Logger
	presetsApplied []string
}

// Config holds configuration for the IP access controller.
type Config struct {
	// AllowedIPs is a comma-separated list of IPs and/or CIDR ranges
	// Example: "192.168.1.100,172.30.32.0/24,10.0.0.0/8"
	AllowedIPs string

	// AllowedPresets is a comma-separated list of preset names
	// Valid presets: localhost, hassio, docker, private
	AllowedPresets string

	// LogDenied logs all denied connection attempts
	LogDenied bool

	// LogAllowed logs all allowed connection attempts
	LogAllowed bool

	// Logger is the zerolog logger to use
	Logger zerolog.Logger
}

// New creates a new IP access controller with the given configuration.
// Returns an error if any IP addresses or CIDR ranges are invalid.
func New(cfg Config) (*Controller, error) {
	c := &Controller{
		logDenied:  cfg.LogDenied,
		logAllowed: cfg.LogAllowed,
		logger:     cfg.Logger,
	}

	// Parse presets first
	if cfg.AllowedPresets != "" {
		presets := strings.Split(cfg.AllowedPresets, ",")
		for _, preset := range presets {
			preset = strings.TrimSpace(strings.ToLower(preset))
			if preset == "" {
				continue
			}

			ranges, ok := Presets[preset]
			if !ok {
				return nil, fmt.Errorf("unknown preset: %s (valid: %s)", preset, validPresetNames())
			}

			for _, cidr := range ranges {
				_, ipNet, err := net.ParseCIDR(cidr)
				if err != nil {
					return nil, fmt.Errorf("invalid CIDR in preset %s: %s: %w", preset, cidr, err)
				}
				c.allowedNets = append(c.allowedNets, ipNet)
			}
			c.presetsApplied = append(c.presetsApplied, preset)
		}
	}

	// Parse explicit IPs/CIDRs
	if cfg.AllowedIPs != "" {
		entries := strings.Split(cfg.AllowedIPs, ",")
		for _, entry := range entries {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}

			if err := c.addEntry(entry); err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}

// addEntry adds a single IP or CIDR to the allowlist.
func (c *Controller) addEntry(entry string) error {
	// Check if it's a CIDR
	if strings.Contains(entry, "/") {
		_, ipNet, err := net.ParseCIDR(entry)
		if err != nil {
			return fmt.Errorf("invalid CIDR: %s: %w", entry, err)
		}
		c.allowedNets = append(c.allowedNets, ipNet)
		return nil
	}

	// It's a single IP
	ip := net.ParseIP(entry)
	if ip == nil {
		return fmt.Errorf("invalid IP address: %s", entry)
	}
	c.allowedIPs = append(c.allowedIPs, ip)
	return nil
}

// IsAllowed checks if the given IP address is allowed.
// The addr can be in the format "IP" or "IP:port".
func (c *Controller) IsAllowed(addr string) bool {
	ip := c.extractIP(addr)
	if ip == nil {
		c.logDeniedAttempt(addr, "invalid IP format")
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if no rules configured (deny all by default for security)
	if len(c.allowedNets) == 0 && len(c.allowedIPs) == 0 {
		c.logDeniedAttempt(addr, "no allowlist configured")
		return false
	}

	// Check explicit IPs
	for _, allowedIP := range c.allowedIPs {
		if ip.Equal(allowedIP) {
			c.logAllowedAttempt(addr, "matched explicit IP")
			return true
		}
	}

	// Check CIDR ranges
	for _, ipNet := range c.allowedNets {
		if ipNet.Contains(ip) {
			c.logAllowedAttempt(addr, fmt.Sprintf("matched CIDR %s", ipNet.String()))
			return true
		}
	}

	c.logDeniedAttempt(addr, "not in allowlist")
	return false
}

// extractIP extracts the IP from an address string (with or without port).
func (c *Controller) extractIP(addr string) net.IP {
	// Try parsing as IP:port first
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Maybe it's just an IP without port
		host = addr
	}

	return net.ParseIP(host)
}

// logDeniedAttempt logs a denied connection attempt if logging is enabled.
func (c *Controller) logDeniedAttempt(addr, reason string) {
	if c.logDenied {
		c.logger.Warn().
			Str("remote_addr", addr).
			Str("reason", reason).
			Msg("connection denied by IP access control")
	}
}

// logAllowedAttempt logs an allowed connection attempt if logging is enabled.
func (c *Controller) logAllowedAttempt(addr, reason string) {
	if c.logAllowed {
		c.logger.Debug().
			Str("remote_addr", addr).
			Str("reason", reason).
			Msg("connection allowed by IP access control")
	}
}

// AddIP dynamically adds an IP to the allowlist.
// Returns nil if the IP was added or already exists.
func (c *Controller) AddIP(ipStr string) error {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return fmt.Errorf("invalid IP address: %s", ipStr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for duplicate
	for _, existingIP := range c.allowedIPs {
		if ip.Equal(existingIP) {
			c.logger.Debug().Str("ip", ipStr).Msg("IP already in allowlist, skipping")
			return nil
		}
	}

	c.allowedIPs = append(c.allowedIPs, ip)
	c.logger.Info().Str("ip", ipStr).Msg("dynamically added IP to allowlist")
	return nil
}

// AddCIDR dynamically adds a CIDR range to the allowlist.
// Returns nil if the CIDR was added or already exists.
func (c *Controller) AddCIDR(cidr string) error {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return fmt.Errorf("invalid CIDR: %s: %w", cidr, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for duplicate (compare canonical string representation)
	newCIDR := ipNet.String()
	for _, existingNet := range c.allowedNets {
		if existingNet.String() == newCIDR {
			c.logger.Debug().Str("cidr", cidr).Msg("CIDR already in allowlist, skipping")
			return nil
		}
	}

	c.allowedNets = append(c.allowedNets, ipNet)
	c.logger.Info().Str("cidr", cidr).Msg("dynamically added CIDR to allowlist")
	return nil
}

// Stats returns statistics about the current allowlist configuration.
func (c *Controller) Stats() (ipCount, cidrCount int, presets []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.allowedIPs), len(c.allowedNets), c.presetsApplied
}

// IsEnabled returns true if any access control rules are configured.
func (c *Controller) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.allowedIPs) > 0 || len(c.allowedNets) > 0
}

// String returns a human-readable description of the access control configuration.
func (c *Controller) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.allowedIPs) == 0 && len(c.allowedNets) == 0 {
		return "IP access control: disabled (no rules configured - denying all)"
	}

	var parts []string
	if len(c.presetsApplied) > 0 {
		parts = append(parts, fmt.Sprintf("presets=%s", strings.Join(c.presetsApplied, ",")))
	}
	if len(c.allowedIPs) > 0 {
		parts = append(parts, fmt.Sprintf("explicit_ips=%d", len(c.allowedIPs)))
	}
	if len(c.allowedNets) > 0 {
		parts = append(parts, fmt.Sprintf("cidr_ranges=%d", len(c.allowedNets)))
	}

	return fmt.Sprintf("IP access control: enabled (%s)", strings.Join(parts, ", "))
}
