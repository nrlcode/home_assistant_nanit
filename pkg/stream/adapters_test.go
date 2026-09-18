package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/notedit/rtmp/av"
)

// ============================================================
// INTERNAL MOCKS FOR ADAPTER TESTS
// These are minimal mocks for testing adapters in the same package.
// ============================================================

type mockLocalStreamer struct {
	RequestErr    error
	WaitForPubErr error
}

func (m *mockLocalStreamer) RequestLocalStream(ctx context.Context) error {
	return m.RequestErr
}

func (m *mockLocalStreamer) StopLocalStream(ctx context.Context) error {
	return nil
}

func (m *mockLocalStreamer) WaitForPublisher(ctx context.Context, timeout time.Duration) error {
	return m.WaitForPubErr
}

type mockRemoteRelay struct {
	StartErr error
	running  bool
}

func (m *mockRemoteRelay) Start(ctx context.Context) error {
	if m.StartErr != nil {
		return m.StartErr
	}
	m.running = true
	return nil
}

func (m *mockRemoteRelay) Stop() error {
	m.running = false
	return nil
}

func (m *mockRemoteRelay) IsRunning() bool {
	return m.running
}

type mockRTMPClient struct {
	ConnectErr error
	ReadErr    error
	Packets    []av.Packet
	idx        int
}

func (m *mockRTMPClient) Connect(url string) error {
	return m.ConnectErr
}

func (m *mockRTMPClient) ReadPacket() (av.Packet, error) {
	if m.ReadErr != nil {
		return av.Packet{}, m.ReadErr
	}
	if m.idx >= len(m.Packets) {
		return av.Packet{}, errors.New("no more packets")
	}
	pkt := m.Packets[m.idx]
	m.idx++
	return pkt, nil
}

func (m *mockRTMPClient) Close() error {
	return nil
}

type mockPacketReceiver struct {
	received []av.Packet
	mu       sync.Mutex
}

func (m *mockPacketReceiver) ReceivePacket(pkt av.Packet) {
	m.mu.Lock()
	m.received = append(m.received, pkt)
	m.mu.Unlock()
}

// ============================================================
// ADAPTER TESTS
// These test that adapters correctly implement our interfaces.
// ============================================================

// --- BroadcasterAdapter Tests ---

func TestBroadcasterAdapter_ImplementsPacketReceiver(t *testing.T) {
	// Verify BroadcasterAdapter implements PacketReceiver interface
	var _ PacketReceiver = (*BroadcasterAdapter)(nil)
}

func TestBroadcasterAdapter_ReceivePacket(t *testing.T) {
	var received []av.Packet
	var mu sync.Mutex

	adapter := NewBroadcasterAdapter(func(pkt av.Packet) {
		mu.Lock()
		received = append(received, pkt)
		mu.Unlock()
	})

	// Send some packets
	testPackets := []av.Packet{
		{IsKeyFrame: true, Data: []byte{1, 2, 3}},
		{IsKeyFrame: false, Data: []byte{4, 5, 6}},
		{IsKeyFrame: false, Data: []byte{7, 8, 9}},
	}

	for _, pkt := range testPackets {
		adapter.ReceivePacket(pkt)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(received) != len(testPackets) {
		t.Errorf("Expected %d packets, got %d", len(testPackets), len(received))
	}

	for i, pkt := range received {
		if pkt.IsKeyFrame != testPackets[i].IsKeyFrame {
			t.Errorf("Packet %d: expected IsKeyFrame=%v, got %v",
				i, testPackets[i].IsKeyFrame, pkt.IsKeyFrame)
		}
	}
}

func TestBroadcasterAdapter_NilFunctionPanics(t *testing.T) {
	// A nil broadcast function would panic - this is expected behavior
	// since we should never create an adapter without a function
	adapter := &BroadcasterAdapter{broadcast: nil}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when calling ReceivePacket with nil broadcast function")
		}
	}()

	adapter.ReceivePacket(av.Packet{})
}

// --- RealRTMPClient Tests ---

func TestRealRTMPClient_ImplementsRTMPClient(t *testing.T) {
	// Verify RealRTMPClient implements RTMPClient interface
	var _ RTMPClient = (*RealRTMPClient)(nil)
}

func TestRealRTMPClient_NewClient(t *testing.T) {
	client := NewRealRTMPClient()
	if client == nil {
		t.Fatal("NewRealRTMPClient returned nil")
	}
	if client.client == nil {
		t.Error("Internal rtmp client not initialized")
	}
	if client.conn != nil {
		t.Error("conn should be nil initially")
	}
	if client.netConn != nil {
		t.Error("netConn should be nil initially")
	}
}

func TestRealRTMPClient_ReadPacketNotConnected(t *testing.T) {
	client := NewRealRTMPClient()

	_, err := client.ReadPacket()
	if err == nil {
		t.Error("Expected error when reading from unconnected client")
	}
	if err.Error() != "not connected" {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}
}

func TestRealRTMPClient_CloseNotConnected(t *testing.T) {
	client := NewRealRTMPClient()

	// Closing an unconnected client should not error
	err := client.Close()
	if err != nil {
		t.Errorf("Expected no error when closing unconnected client, got: %v", err)
	}
}

func TestRealRTMPClient_CloseIdempotent(t *testing.T) {
	client := NewRealRTMPClient()

	// Multiple closes should be safe
	for i := 0; i < 3; i++ {
		err := client.Close()
		if err != nil {
			t.Errorf("Close call %d: unexpected error: %v", i+1, err)
		}
	}
}

// --- LocalStreamerAdapter Tests ---
// Note: LocalStreamerAdapter requires actual WebSocket connection and StateManager
// so we test what we can without those dependencies.

func TestLocalStreamerAdapter_ImplementsLocalStreamer(t *testing.T) {
	// Verify LocalStreamerAdapter implements LocalStreamer interface
	var _ LocalStreamer = (*LocalStreamerAdapter)(nil)
}

// --- Interface Satisfaction Tests ---

func TestMockLocalStreamer_ImplementsInterface(t *testing.T) {
	var _ LocalStreamer = (*mockLocalStreamer)(nil)
}

func TestMockRemoteRelay_ImplementsInterface(t *testing.T) {
	var _ RemoteRelay = (*mockRemoteRelay)(nil)
}

func TestMockRTMPClient_ImplementsInterface(t *testing.T) {
	var _ RTMPClient = (*mockRTMPClient)(nil)
}

func TestMockPacketReceiver_ImplementsInterface(t *testing.T) {
	var _ PacketReceiver = (*mockPacketReceiver)(nil)
}

// --- Integration-like Tests ---

func TestBroadcasterAdapterIntegration_ManyPackets(t *testing.T) {
	// Test that adapter handles many packets without issues
	const numPackets = 1000
	receivedCount := 0
	var mu sync.Mutex

	adapter := NewBroadcasterAdapter(func(pkt av.Packet) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
	})

	for i := 0; i < numPackets; i++ {
		adapter.ReceivePacket(av.Packet{
			IsKeyFrame: i%30 == 0, // Key frame every 30 packets
			Data:       []byte{byte(i % 256)},
		})
	}

	mu.Lock()
	defer mu.Unlock()

	if receivedCount != numPackets {
		t.Errorf("Expected %d packets received, got %d", numPackets, receivedCount)
	}
}

func TestBroadcasterAdapterIntegration_ConcurrentPackets(t *testing.T) {
	// Test that adapter handles concurrent packet sends
	const numGoroutines = 10
	const packetsPerGoroutine = 100
	receivedCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	adapter := NewBroadcasterAdapter(func(pkt av.Packet) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
	})

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < packetsPerGoroutine; i++ {
				adapter.ReceivePacket(av.Packet{
					Data: []byte{byte(goroutineID), byte(i)},
				})
			}
		}(g)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	expected := numGoroutines * packetsPerGoroutine
	if receivedCount != expected {
		t.Errorf("Expected %d packets received, got %d", expected, receivedCount)
	}
}

// --- ErrMaxConnections Tests ---

func TestErrMaxConnections_IsError(t *testing.T) {
	if ErrMaxConnections == nil {
		t.Fatal("ErrMaxConnections should not be nil")
	}

	var err error = ErrMaxConnections
	if !errors.Is(err, ErrMaxConnections) {
		t.Error("errors.Is should match ErrMaxConnections")
	}
}

// --- RTMPRelay Adapter Integration Test ---

func TestRTMPRelay_UsesRTMPClientInterface(t *testing.T) {
	// Verify RTMPRelay can work with any RTMPClient implementation
	client := &mockRTMPClient{}
	receiver := &mockPacketReceiver{}

	relay := NewRemoteRelay(RemoteRelayConfig{
		BabyUID:        "test-baby",
		AuthToken:      "test-token",
		RTMPClient:     client,
		PacketReceiver: receiver,
	})

	if relay == nil {
		t.Fatal("NewRemoteRelay returned nil")
	}

	// The relay was created successfully with mock dependencies
	// Actual relay behavior is tested in relay_test.go
}

// --- StreamManager Integration Test ---

func TestStreamManager_UsesAllInterfaces(t *testing.T) {
	// Verify StreamManager can be created with mock implementations
	local := &mockLocalStreamer{}
	relay := &mockRemoteRelay{}
	stateManager := baby.NewStateManager()

	// Simulate success for first local stream
	local.RequestErr = nil

	sm := NewStreamManager(StreamManagerConfig{
		BabyUID:             "test-baby",
		StateManager:        stateManager,
		LocalStreamer:       local,
		RemoteRelay:         relay,
		RestoreInitialDelay: 0, // Disabled for this test
		RestoreMaxDelay:     0,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx)
	if err != nil {
		t.Fatalf("StreamManager.Start failed: %v", err)
	}

	sm.Stop()
}
