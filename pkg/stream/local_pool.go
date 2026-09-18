package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/rtmpserver"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// LocalStreamPool manages 2 hot standby local streams for seamless failover.
// When a stall is detected on the active stream, it switches to the standby.
type LocalStreamPool struct {
	babyUID      string
	baseURL      string // Base URL without slot suffix (e.g., rtmp://host:port/local/babyUID)
	conn         *client.WebsocketConnection
	stateManager *baby.StateManager
	rtmpServer   *rtmpserver.RTMPServer

	mu            sync.RWMutex
	activeSlot    int     // 0 or 1 - which slot is currently active
	slotActive    [2]bool // Whether each slot has an active publisher
	slotRequested [2]bool // Whether we've requested streaming for each slot
	started       bool
	ctx           context.Context
	cancel        context.CancelFunc

	// Stall detection config
	stallThreshold time.Duration

	// Callbacks
	onBothFailed func() // Called when both local streams fail
}

// LocalStreamPoolConfig configuration for LocalStreamPool
type LocalStreamPoolConfig struct {
	BabyUID        string
	BaseURL        string
	Conn           *client.WebsocketConnection
	StateManager   *baby.StateManager
	RTMPServer     *rtmpserver.RTMPServer
	StallThreshold time.Duration
	OnBothFailed   func() // Callback when both local streams fail
}

// NewLocalStreamPool creates a new local stream pool
func NewLocalStreamPool(config LocalStreamPoolConfig) *LocalStreamPool {
	if config.StallThreshold == 0 {
		config.StallThreshold = 4 * time.Second // Default 4 second stall threshold
	}

	return &LocalStreamPool{
		babyUID:        config.BabyUID,
		baseURL:        config.BaseURL,
		conn:           config.Conn,
		stateManager:   config.StateManager,
		rtmpServer:     config.RTMPServer,
		stallThreshold: config.StallThreshold,
		onBothFailed:   config.OnBothFailed,
	}
}

// slotSourceID returns the RTMP source ID for a slot (used for timestamp remapping)
func (p *LocalStreamPool) slotSourceID(slot int) int {
	// Use source IDs 10, 11 for local pool slots (distinct from SourceIDLocal=1, SourceIDRemote=2)
	return 10 + slot
}

// slotURL returns the RTMP URL for a specific slot
func (p *LocalStreamPool) slotURL(slot int) string {
	return fmt.Sprintf("%s_%d", p.baseURL, slot)
}

// RequestLocalStream implements LocalStreamer interface - starts both streams
func (p *LocalStreamPool) RequestLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true
	p.activeSlot = 0
	p.mu.Unlock()

	// Configure stall detection on the broadcaster
	p.configureStallDetection()

	// Request stream for slot 0 (primary)
	if err := p.requestSlotStream(0); err != nil {
		return err
	}

	// Request stream for slot 1 (standby) in background
	go func() {
		time.Sleep(2 * time.Second) // Small delay to avoid overwhelming camera
		p.requestSlotStream(1)
	}()

	return nil
}

// configureStallDetection sets up the stall callback on the broadcaster
func (p *LocalStreamPool) configureStallDetection() {
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster == nil {
		log.Warn().Str("baby_uid", p.babyUID).Msg("No broadcaster found for stall detection")
		return
	}

	broadcaster.SetStallConfig(&rtmpserver.StallConfig{
		Threshold: p.stallThreshold,
		Callback:  p.onStallDetected,
	})

	// Set initial active source to slot 0
	broadcaster.SetActiveSource(p.slotSourceID(0))

	log.Info().
		Str("baby_uid", p.babyUID).
		Dur("threshold", p.stallThreshold).
		Msg("Configured stall detection for local stream pool")
}

// onStallDetected is called when the broadcaster detects a stall
func (p *LocalStreamPool) onStallDetected(sourceID int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	// Determine which slot stalled based on sourceID
	stalledSlot := -1
	for i := 0; i < 2; i++ {
		if p.slotSourceID(i) == sourceID {
			stalledSlot = i
			break
		}
	}

	// If stall is from unknown source or not our active slot, ignore
	if stalledSlot == -1 || stalledSlot != p.activeSlot {
		log.Debug().
			Str("baby_uid", p.babyUID).
			Int("source_id", sourceID).
			Int("active_slot", p.activeSlot).
			Msg("Stall detected on non-active slot, ignoring")
		return
	}

	standbySlot := 1 - p.activeSlot

	log.Warn().
		Str("baby_uid", p.babyUID).
		Int("stalled_slot", stalledSlot).
		Int("standby_slot", standbySlot).
		Bool("standby_active", p.slotActive[standbySlot]).
		Msg("Stall detected on active local stream")

	// Check if standby is available
	if p.slotActive[standbySlot] {
		// Switch to standby
		p.activeSlot = standbySlot

		// Update active source on broadcaster
		if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
			broadcaster.SetActiveSource(p.slotSourceID(standbySlot))
		}

		log.Info().
			Str("baby_uid", p.babyUID).
			Int("new_active_slot", standbySlot).
			Msg("Switched to standby local stream")

		// Mark old slot as inactive and request new stream for it
		p.slotActive[stalledSlot] = false
		go p.requestSlotStream(stalledSlot)
	} else {
		// No standby available
		log.Warn().
			Str("baby_uid", p.babyUID).
			Msg("No standby stream available, both local streams may be failing")

		// Try to request standby anyway
		if !p.slotRequested[standbySlot] {
			go p.requestSlotStream(standbySlot)
		}

		// If both slots are not active, trigger failover callback
		if !p.slotActive[0] && !p.slotActive[1] {
			if p.onBothFailed != nil {
				go p.onBothFailed()
			}
		}
	}
}

// requestSlotStream requests streaming for a specific slot
func (p *LocalStreamPool) requestSlotStream(slot int) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return fmt.Errorf("pool not started")
	}
	p.slotRequested[slot] = true
	p.mu.Unlock()

	url := p.slotURL(slot)
	log.Info().
		Str("baby_uid", p.babyUID).
		Int("slot", slot).
		Str("url", url).
		Msg("Requesting local stream for slot")

	awaitResponse := p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(url),
			Status:   client.Streaming_Status(client.Streaming_STARTED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	_, err := awaitResponse(30 * time.Second)
	if err != nil {
		log.Error().
			Err(err).
			Str("baby_uid", p.babyUID).
			Int("slot", slot).
			Msg("Failed to request local stream for slot")

		p.mu.Lock()
		p.slotRequested[slot] = false
		p.mu.Unlock()
		return err
	}

	log.Info().
		Str("baby_uid", p.babyUID).
		Int("slot", slot).
		Msg("Local stream requested successfully for slot")

	// Wait for publisher to connect
	timeout := 15 * time.Second
	err = p.rtmpServer.WaitForPublisherWithURL(p.babyUID, p.slotURL(slot), timeout)
	if err != nil {
		log.Warn().
			Err(err).
			Str("baby_uid", p.babyUID).
			Int("slot", slot).
			Msg("Publisher didn't connect for slot")

		p.mu.Lock()
		p.slotRequested[slot] = false
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	p.slotActive[slot] = true
	otherSlot := 1 - slot
	otherSlotActive := p.slotActive[otherSlot]
	currentActiveSlot := p.activeSlot

	// If this slot is now the only active one, switch to it
	shouldSwitch := !otherSlotActive && currentActiveSlot != slot
	if shouldSwitch {
		p.activeSlot = slot
	}
	p.mu.Unlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Int("slot", slot).
		Msg("Local stream active for slot")

	// Update broadcaster's active source if we switched
	if shouldSwitch {
		if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
			broadcaster.SetActiveSource(p.slotSourceID(slot))
		}
		log.Info().
			Str("baby_uid", p.babyUID).
			Int("slot", slot).
			Msg("Switched active source to newly available slot")
	}

	// Update state - this slot is now active or we just switched to it
	p.mu.RLock()
	isActive := slot == p.activeSlot
	p.mu.RUnlock()

	if isActive {
		p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))
	}

	return nil
}

// WaitForPublisher implements LocalStreamer interface
func (p *LocalStreamPool) WaitForPublisher(ctx context.Context, timeout time.Duration) error {
	p.mu.RLock()
	activeSlot := p.activeSlot
	p.mu.RUnlock()

	// Wait for the active slot's publisher
	return p.rtmpServer.WaitForPublisherWithURL(p.babyUID, p.slotURL(activeSlot), timeout)
}

// StopLocalStream implements LocalStreamer interface
func (p *LocalStreamPool) StopLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = false
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Unlock()

	// Stop stall detection
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster != nil {
		broadcaster.SetStallConfig(nil)
	}

	// Stop both slots
	for slot := 0; slot < 2; slot++ {
		url := p.slotURL(slot)
		p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
			Streaming: &client.Streaming{
				Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
				RtmpUrl:  utils.ConstRefStr(url),
				Status:   client.Streaming_Status(client.Streaming_STOPPED).Enum(),
				Attempts: utils.ConstRefInt32(1),
			},
		})
	}

	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream pool stopped")
	return nil
}

// OnPublisherConnected should be called when a publisher connects to track slot status
func (p *LocalStreamPool) OnPublisherConnected(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for slot := 0; slot < 2; slot++ {
		if p.slotURL(slot) == url {
			p.slotActive[slot] = true
			log.Debug().
				Str("baby_uid", p.babyUID).
				Int("slot", slot).
				Msg("Publisher connected for slot")
			return
		}
	}
}

// OnPublisherDisconnected should be called when a publisher disconnects
func (p *LocalStreamPool) OnPublisherDisconnected(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for slot := 0; slot < 2; slot++ {
		if p.slotURL(slot) == url {
			wasActive := p.slotActive[slot]
			p.slotActive[slot] = false
			p.slotRequested[slot] = false

			log.Info().
				Str("baby_uid", p.babyUID).
				Int("slot", slot).
				Bool("was_active", wasActive).
				Msg("Publisher disconnected for slot")

			// If this was the active slot, try to switch to standby
			if slot == p.activeSlot && p.started {
				standbySlot := 1 - slot
				if p.slotActive[standbySlot] {
					p.activeSlot = standbySlot
					log.Info().
						Str("baby_uid", p.babyUID).
						Int("new_active_slot", standbySlot).
						Msg("Switched to standby after publisher disconnect")
				} else {
					// Both failed
					log.Warn().
						Str("baby_uid", p.babyUID).
						Msg("Both local stream slots disconnected")

					if p.onBothFailed != nil {
						go p.onBothFailed()
					}
				}

				// Try to restart the failed slot
				go p.requestSlotStream(slot)
			}
			return
		}
	}
}

// GetActiveSlot returns the currently active slot number
func (p *LocalStreamPool) GetActiveSlot() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeSlot
}

// IsSlotActive returns whether a specific slot has an active publisher
func (p *LocalStreamPool) IsSlotActive(slot int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if slot < 0 || slot > 1 {
		return false
	}
	return p.slotActive[slot]
}
