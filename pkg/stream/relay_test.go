package stream_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/stream"
	"github.com/notedit/rtmp/av"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// TESTS FOR REMOTE RELAY
// These tests define the expected behavior for the RemoteRelay
// which pulls stream from Nanit cloud and pushes to local broadcaster.
// Following TDD, these tests are written BEFORE implementation.
// ============================================================

// --- Mock Dependencies ---

// MockRTMPClient simulates RTMP client connection
type MockRTMPClient struct {
	mu          sync.Mutex
	connected   bool
	connectErr  error
	packets     []av.Packet
	packetIndex int
	readErr     error
	closed      bool
	onConnect   func()
	onClose     func()
}

func (m *MockRTMPClient) Connect(url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	if m.onConnect != nil {
		m.onConnect()
	}
	return nil
}

func (m *MockRTMPClient) ReadPacket() (av.Packet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.readErr != nil {
		return av.Packet{}, m.readErr
	}
	if m.packetIndex >= len(m.packets) {
		// Block until closed or more packets
		return av.Packet{}, errors.New("no more packets")
	}
	pkt := m.packets[m.packetIndex]
	m.packetIndex++
	return pkt, nil
}

func (m *MockRTMPClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.connected = false
	if m.onClose != nil {
		m.onClose()
	}
	return nil
}

func (m *MockRTMPClient) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *MockRTMPClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockRTMPClient) SetPackets(pkts []av.Packet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.packets = pkts
	m.packetIndex = 0
}

// MockPacketReceiver receives packets from the relay
type MockPacketReceiver struct {
	mu       sync.Mutex
	packets  []av.Packet
	onPacket func(av.Packet)
}

func (m *MockPacketReceiver) ReceivePacket(pkt av.Packet) {
	m.mu.Lock()
	m.packets = append(m.packets, pkt)
	onPacket := m.onPacket
	m.mu.Unlock()

	if onPacket != nil {
		onPacket(pkt)
	}
}

func (m *MockPacketReceiver) GetPackets() []av.Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]av.Packet, len(m.packets))
	copy(result, m.packets)
	return result
}

// --- Test Cases ---

func TestRemoteRelayURLConstruction(t *testing.T) {
	url := stream.BuildRemoteStreamURL("baby123", "authtoken456")
	assert.Equal(t, "rtmps://media-secured.nanit.com/nanit/baby123.authtoken456", url)
}

func TestRemoteRelayURLConstructionSpecialChars(t *testing.T) {
	// Auth tokens might contain various characters
	url := stream.BuildRemoteStreamURL("baby-123_abc", "token+with/special=chars")
	assert.Contains(t, url, "baby-123_abc")
	assert.Contains(t, url, "token+with/special=chars")
}

func TestNewRemoteRelay(t *testing.T) {
	receiver := &MockPacketReceiver{}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
	})

	assert.NotNil(t, relay)
	assert.False(t, relay.IsRunning())
}

func TestRemoteRelayStartStop(t *testing.T) {
	receiver := &MockPacketReceiver{}
	client := &MockRTMPClient{
		packets: []av.Packet{
			{Type: 1, Data: []byte{0x01}},
		},
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := relay.Start(ctx)
	require.NoError(t, err)
	assert.True(t, relay.IsRunning())

	err = relay.Stop()
	require.NoError(t, err)
	assert.False(t, relay.IsRunning())
}

func TestRemoteRelayConnectError(t *testing.T) {
	receiver := &MockPacketReceiver{}
	client := &MockRTMPClient{
		connectErr: errors.New("connection refused"),
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx := context.Background()
	err := relay.Start(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.False(t, relay.IsRunning())
}

func TestRemoteRelayReceivesPackets(t *testing.T) {
	packetChan := make(chan av.Packet, 10)
	receiver := &MockPacketReceiver{
		onPacket: func(pkt av.Packet) {
			packetChan <- pkt
		},
	}

	testPackets := []av.Packet{
		{Type: 1, Data: []byte{0x01}},
		{Type: 2, Data: []byte{0x02}},
		{Type: 3, Data: []byte{0x03}},
	}

	client := &MockRTMPClient{
		packets: testPackets,
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := relay.Start(ctx)
	require.NoError(t, err)

	// Wait for packets
	received := make([]av.Packet, 0)
	timeout := time.After(1 * time.Second)

	for len(received) < len(testPackets) {
		select {
		case pkt := <-packetChan:
			received = append(received, pkt)
		case <-timeout:
			t.Fatalf("Timeout waiting for packets, got %d of %d", len(received), len(testPackets))
		}
	}

	assert.Len(t, received, len(testPackets))
	for i, pkt := range received {
		assert.Equal(t, testPackets[i].Type, pkt.Type)
		assert.Equal(t, testPackets[i].Data, pkt.Data)
	}
}

func TestRemoteRelayContextCancellation(t *testing.T) {
	receiver := &MockPacketReceiver{}
	closeCalled := atomic.Bool{}

	client := &MockRTMPClient{
		packets: []av.Packet{{Type: 1}},
		onClose: func() {
			closeCalled.Store(true)
		},
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx, cancel := context.WithCancel(context.Background())

	err := relay.Start(ctx)
	require.NoError(t, err)

	// Cancel context
	cancel()

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	assert.True(t, closeCalled.Load(), "Client should be closed on context cancellation")
}

func TestRemoteRelayReconnectOnError(t *testing.T) {
	receiver := &MockPacketReceiver{}
	connectCount := atomic.Int32{}

	client := &MockRTMPClient{
		packets: []av.Packet{{Type: 1}},
	}

	originalConnect := client.Connect
	client.onConnect = func() {
		connectCount.Add(1)
	}
	_ = originalConnect // Avoid unused warning

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:           "baby123",
		AuthToken:         "authtoken",
		PacketReceiver:    receiver,
		RTMPClient:        client,
		ReconnectDelay:    50 * time.Millisecond,
		MaxReconnectDelay: 200 * time.Millisecond,
		ReconnectEnabled:  true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start in goroutine since Start() blocks while relay is running
	go relay.Start(ctx)

	// Wait for initial connection
	time.Sleep(100 * time.Millisecond)

	// Simulate read error by setting readErr
	client.mu.Lock()
	client.readErr = errors.New("stream ended")
	client.mu.Unlock()

	// Wait for reconnect attempts
	time.Sleep(300 * time.Millisecond)

	// Stop the relay to prevent infinite reconnection loop
	relay.Stop()

	// Should have multiple connect attempts due to reconnection
	assert.GreaterOrEqual(t, connectCount.Load(), int32(2), "Should have reconnected")
}

func TestRemoteRelayStopDuringReconnect(t *testing.T) {
	receiver := &MockPacketReceiver{}
	client := &MockRTMPClient{
		connectErr: errors.New("connection refused"),
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:          "baby123",
		AuthToken:        "authtoken",
		PacketReceiver:   receiver,
		RTMPClient:       client,
		ReconnectDelay:   50 * time.Millisecond,
		ReconnectEnabled: true,
	})

	ctx := context.Background()

	// Start will fail to connect but should still be considered "started"
	// because reconnection is enabled
	go relay.Start(ctx)

	// Wait a bit then stop
	time.Sleep(100 * time.Millisecond)

	err := relay.Stop()
	require.NoError(t, err)

	// Should stop cleanly
	assert.False(t, relay.IsRunning())
}

func TestRemoteRelayDoubleStart(t *testing.T) {
	receiver := &MockPacketReceiver{}
	client := &MockRTMPClient{
		packets: []av.Packet{{Type: 1}},
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx := context.Background()

	err1 := relay.Start(ctx)
	require.NoError(t, err1)

	err2 := relay.Start(ctx)
	require.Error(t, err2) // Should error on double start

	relay.Stop()
}

func TestRemoteRelayDoubleStop(t *testing.T) {
	receiver := &MockPacketReceiver{}
	client := &MockRTMPClient{
		packets: []av.Packet{{Type: 1}},
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx := context.Background()
	relay.Start(ctx)

	err1 := relay.Stop()
	require.NoError(t, err1)

	err2 := relay.Stop()
	require.NoError(t, err2) // Should be idempotent
}

// BlockingMockRTMPClient simulates a long-running RTMP stream that blocks on ReadPacket
type BlockingMockRTMPClient struct {
	mu         sync.Mutex
	connected  bool
	packetChan chan av.Packet
	closed     bool
	closedChan chan struct{}
}

func NewBlockingMockRTMPClient() *BlockingMockRTMPClient {
	return &BlockingMockRTMPClient{
		packetChan: make(chan av.Packet),
		closedChan: make(chan struct{}),
	}
}

func (m *BlockingMockRTMPClient) Connect(url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	return nil
}

func (m *BlockingMockRTMPClient) ReadPacket() (av.Packet, error) {
	select {
	case pkt := <-m.packetChan:
		return pkt, nil
	case <-m.closedChan:
		return av.Packet{}, errors.New("connection closed")
	}
}

func (m *BlockingMockRTMPClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.closedChan)
	}
	return nil
}

func TestRemoteRelayStartNonBlocking(t *testing.T) {
	// This test verifies that Start() returns quickly without blocking
	// even when the RTMP client is streaming continuously
	receiver := &MockPacketReceiver{}
	client := NewBlockingMockRTMPClient() // Uses blocking client that simulates infinite stream

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:        "baby123",
		AuthToken:      "authtoken",
		PacketReceiver: receiver,
		RTMPClient:     client,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start() should return within a reasonable time, not block while streaming
	startDone := make(chan error, 1)
	go func() {
		startDone <- relay.Start(ctx)
	}()

	select {
	case err := <-startDone:
		require.NoError(t, err)
		// Start returned - good!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Start() blocked for too long - it should return immediately after starting the relay loop")
	}

	// Verify relay is running
	assert.True(t, relay.IsRunning())

	// Clean up
	relay.Stop()
}

func TestRemoteRelayUnstableConnectionBackoff(t *testing.T) {
	// This test verifies that unstable connections (connect succeeds but read fails quickly)
	// trigger exponential backoff, preventing rapid reconnection loops
	receiver := &MockPacketReceiver{}
	connectTimes := make([]time.Time, 0)
	connectMu := sync.Mutex{}

	// Client that connects successfully but fails to read immediately
	client := &MockRTMPClient{
		packets: []av.Packet{}, // No packets - will immediately fail
	}

	client.onConnect = func() {
		connectMu.Lock()
		connectTimes = append(connectTimes, time.Now())
		connectMu.Unlock()
	}

	relay := stream.NewRemoteRelay(stream.RemoteRelayConfig{
		BabyUID:           "baby123",
		AuthToken:         "authtoken",
		PacketReceiver:    receiver,
		RTMPClient:        client,
		ReconnectDelay:    50 * time.Millisecond,
		MaxReconnectDelay: 200 * time.Millisecond,
		ReconnectEnabled:  true,
	})

	ctx, cancel := context.WithCancel(context.Background())

	go relay.Start(ctx)

	// Wait for several reconnection attempts
	time.Sleep(600 * time.Millisecond)
	cancel()
	relay.Stop()

	connectMu.Lock()
	defer connectMu.Unlock()

	// Should have multiple attempts
	require.GreaterOrEqual(t, len(connectTimes), 3, "Should have at least 3 connection attempts")

	// Verify exponential backoff: delays should increase
	// First delay: ~50ms, second: ~100ms, third: ~200ms (capped)
	if len(connectTimes) >= 3 {
		delay1 := connectTimes[1].Sub(connectTimes[0])
		delay2 := connectTimes[2].Sub(connectTimes[1])

		// Second delay should be longer than first (with some tolerance for timing)
		assert.Greater(t, delay2.Milliseconds(), delay1.Milliseconds()/2,
			"Backoff should increase: delay1=%v, delay2=%v", delay1, delay2)
	}
}
