package stream

import (
	"context"
)

// NoopRelay is a no-op implementation of RemoteRelay.
// Used when the HybridStreamPool handles remote relay internally.
type NoopRelay struct{}

// NewNoopRelay creates a new no-op relay
func NewNoopRelay() *NoopRelay {
	return &NoopRelay{}
}

// Start does nothing - returns nil immediately
func (r *NoopRelay) Start(ctx context.Context) error {
	// Block until context is done (so StreamManager doesn't spam restarts)
	<-ctx.Done()
	return ctx.Err()
}

// Stop does nothing
func (r *NoopRelay) Stop() error {
	return nil
}
