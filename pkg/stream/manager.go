package stream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/rs/zerolog/log"
)

// ErrMaxConnections is returned when the camera has too many mobile app connections
var ErrMaxConnections = errors.New("max connections reached")

// ErrPublisherTimeout is returned when the camera doesn't connect within timeout
var ErrPublisherTimeout = errors.New("publisher connection timeout")

// IsMaxConnectionsError checks if an error indicates max connections limit
func IsMaxConnectionsError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrMaxConnections) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connections above limit")
}

// IsPublisherTimeoutError checks if an error indicates publisher timeout
func IsPublisherTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPublisherTimeout) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "publisher connection timeout")
}

// ShouldFailoverToRemote returns true if the error should trigger remote failover
func ShouldFailoverToRemote(err error) bool {
	return IsMaxConnectionsError(err) || IsPublisherTimeoutError(err)
}

// LocalStreamer interface for requesting local streaming from camera
type LocalStreamer interface {
	RequestLocalStream(ctx context.Context) error
	StopLocalStream(ctx context.Context) error
	// WaitForPublisher waits for the camera to connect to the RTMP server
	WaitForPublisher(ctx context.Context, timeout time.Duration) error
}

// RemoteRelay interface for pulling and relaying remote stream
type RemoteRelay interface {
	Start(ctx context.Context) error
	Stop() error
}

// StreamManagerConfig configuration for StreamManager
type StreamManagerConfig struct {
	BabyUID       string
	StateManager  *baby.StateManager
	LocalStreamer LocalStreamer
	RemoteRelay   RemoteRelay

	// Restore timing configuration
	RestoreInitialDelay time.Duration
	RestoreMaxDelay     time.Duration
	RestoreMultiplier   float64

	// Publisher connection timeout (default 15s)
	PublisherTimeout time.Duration

	// PreferRemote starts with remote stream first, then tries to switch to local
	// Useful for testing source switching behavior
	PreferRemote bool
}

// StreamManager manages local/remote stream failover with auto-restoration
type StreamManager struct {
	config StreamManagerConfig

	mu          sync.RWMutex
	currentType baby.StreamType
	ctx         context.Context
	cancel      context.CancelFunc
	restoreWg   sync.WaitGroup
}

// NewStreamManager creates a new StreamManager
func NewStreamManager(config StreamManagerConfig) *StreamManager {
	// Set defaults
	if config.RestoreInitialDelay == 0 {
		config.RestoreInitialDelay = 30 * time.Second
	}
	if config.RestoreMaxDelay == 0 {
		config.RestoreMaxDelay = 5 * time.Minute
	}
	if config.RestoreMultiplier == 0 {
		config.RestoreMultiplier = 2.0
	}
	if config.PublisherTimeout == 0 {
		config.PublisherTimeout = 15 * time.Second
	}

	return &StreamManager{
		config:      config,
		currentType: baby.StreamType_None,
	}
}

// GetCurrentType returns the current stream type
func (sm *StreamManager) GetCurrentType() baby.StreamType {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentType
}

func (sm *StreamManager) setCurrentType(streamType baby.StreamType) {
	sm.mu.Lock()
	sm.currentType = streamType
	sm.mu.Unlock()

	// Update state manager
	sm.config.StateManager.Update(sm.config.BabyUID,
		*baby.NewState().SetStreamType(streamType))
}

// Start begins streaming, preferring local with remote fallback (or vice versa if PreferRemote)
func (sm *StreamManager) Start(ctx context.Context) error {
	sm.mu.Lock()
	sm.ctx, sm.cancel = context.WithCancel(ctx)
	sm.mu.Unlock()

	// If PreferRemote, start remote first then try to switch to local
	if sm.config.PreferRemote {
		return sm.startWithRemote()
	}

	return sm.startWithLocal()
}

// startWithLocal tries local first, falls back to remote
func (sm *StreamManager) startWithLocal() error {
	// Try local first
	err := sm.config.LocalStreamer.RequestLocalStream(sm.ctx)
	if err != nil {
		// Request itself failed
		if !ShouldFailoverToRemote(err) {
			log.Error().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Local streaming request failed")
			return err
		}
		log.Warn().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Failing over to remote")
	} else {
		// Request succeeded, wait for publisher to actually connect
		log.Debug().Str("baby_uid", sm.config.BabyUID).
			Dur("timeout", sm.config.PublisherTimeout).
			Msg("Waiting for camera to connect to RTMP server")

		err = sm.config.LocalStreamer.WaitForPublisher(sm.ctx, sm.config.PublisherTimeout)
		if err == nil {
			sm.setCurrentType(baby.StreamType_Local)
			log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Local streaming started")

			// Start monitoring for local stream drops
			sm.restoreWg.Add(1)
			go sm.monitorLocalStream()

			return nil
		}

		// Publisher didn't connect in time
		log.Warn().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Camera didn't connect, failing over to remote")
	}

	// Failover to remote
	err = sm.config.RemoteRelay.Start(sm.ctx)
	if err != nil {
		log.Error().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Remote streaming failed")
		return err
	}

	sm.setCurrentType(baby.StreamType_Remote)
	log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Remote streaming started")

	// Start background restoration task
	sm.restoreWg.Add(1)
	go sm.restoreLocalLoop()

	return nil
}

// startWithRemote starts remote first, then tries to switch to local
func (sm *StreamManager) startWithRemote() error {
	log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Starting with remote stream (PreferRemote mode)")

	// Start remote first
	err := sm.config.RemoteRelay.Start(sm.ctx)
	if err != nil {
		log.Error().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Remote streaming failed")
		return err
	}

	sm.setCurrentType(baby.StreamType_Remote)
	log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Remote streaming started")

	// Start background task to switch to local
	sm.restoreWg.Add(1)
	go sm.restoreLocalLoop()

	return nil
}

// monitorLocalStream watches for local stream drops and fails over to remote
func (sm *StreamManager) monitorLocalStream() {
	defer sm.restoreWg.Done()

	// Create a channel to receive state updates
	stateChanged := make(chan baby.StreamState, 10)

	// Subscribe to state changes
	unsubscribe := sm.config.StateManager.Subscribe(func(babyUID string, state baby.State) {
		// Only care about our baby and stream state changes
		if babyUID != sm.config.BabyUID {
			return
		}
		if state.StreamState != nil {
			select {
			case stateChanged <- *state.StreamState:
			default:
				// Channel full, skip (we'll get the next one)
			}
		}
	})
	defer unsubscribe()

	for {
		select {
		case <-sm.ctx.Done():
			return

		case streamState := <-stateChanged:
			// If we're on local and stream became unhealthy, failover
			if sm.GetCurrentType() == baby.StreamType_Local && streamState == baby.StreamState_Unhealthy {
				log.Warn().Str("baby_uid", sm.config.BabyUID).Msg("Local stream dropped, failing over to remote")

				// Failover to remote
				err := sm.config.RemoteRelay.Start(sm.ctx)
				if err != nil {
					log.Error().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Failed to start remote stream after local drop")
					// Keep monitoring, maybe local will recover
					continue
				}

				sm.setCurrentType(baby.StreamType_Remote)
				log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Switched to remote streaming after local drop")

				// Start restore loop to get back to local
				sm.restoreWg.Add(1)
				go sm.restoreLocalLoop()

				// Our job here is done - restore loop will handle switching back
				return
			}
		}
	}
}

// restoreLocalLoop periodically tries to restore local streaming while on remote
func (sm *StreamManager) restoreLocalLoop() {
	defer sm.restoreWg.Done()

	delay := sm.config.RestoreInitialDelay

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-time.After(delay):
		}

		// Check if we're still on remote
		if sm.GetCurrentType() != baby.StreamType_Remote {
			return
		}

		log.Debug().Str("baby_uid", sm.config.BabyUID).Msg("Attempting to restore local streaming")

		err := sm.config.LocalStreamer.RequestLocalStream(sm.ctx)
		if err != nil {
			if ShouldFailoverToRemote(err) {
				log.Debug().Str("baby_uid", sm.config.BabyUID).Msg("Local still unavailable, will retry")
			} else {
				log.Warn().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Restore attempt failed with unexpected error")
			}
		} else {
			// Request succeeded, wait for publisher
			err = sm.config.LocalStreamer.WaitForPublisher(sm.ctx, sm.config.PublisherTimeout)
			if err == nil {
				// Success! Switch back to local
				log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Local streaming restored")

				// Stop remote relay
				sm.config.RemoteRelay.Stop()

				sm.setCurrentType(baby.StreamType_Local)

				// Restart monitoring for future local stream drops
				sm.restoreWg.Add(1)
				go sm.monitorLocalStream()

				return
			}
			log.Debug().Err(err).Str("baby_uid", sm.config.BabyUID).Msg("Camera didn't reconnect, will retry")
		}

		// Increase delay with backoff
		delay = time.Duration(float64(delay) * sm.config.RestoreMultiplier)
		if delay > sm.config.RestoreMaxDelay {
			delay = sm.config.RestoreMaxDelay
		}
	}
}

// Stop stops streaming and cleanup
func (sm *StreamManager) Stop() {
	sm.mu.Lock()
	if sm.cancel != nil {
		sm.cancel()
	}
	sm.mu.Unlock()

	// Wait for restore loop to finish
	sm.restoreWg.Wait()

	// Stop both sides regardless of logical source: the App-selected
	// LocalStreamer is the owning HybridStreamPool while RemoteRelay may be
	// a NoopRelay, so stopping only the logical side would leave workers.
	sm.config.LocalStreamer.StopLocalStream(context.Background())
	sm.config.RemoteRelay.Stop()

	sm.setCurrentType(baby.StreamType_None)
	log.Info().Str("baby_uid", sm.config.BabyUID).Msg("Streaming stopped")
}
