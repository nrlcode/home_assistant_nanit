package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/rtmpserver"
	"github.com/notedit/rtmp/av"
)

// fakeRTMPClient is a synthetic transport: synchronous Connect success,
// blocking ReadPacket until Close so the relay worker stays alive.
type fakeRTMPClient struct {
	closedCh chan struct{}
}

func newFakeRTMPClient() *fakeRTMPClient { return &fakeRTMPClient{closedCh: make(chan struct{})} }

func (f *fakeRTMPClient) Connect(url string) error { return nil }

func (f *fakeRTMPClient) ReadPacket() (av.Packet, error) {
	<-f.closedCh
	return av.Packet{}, errors.New("closed")
}

func (f *fakeRTMPClient) Close() error {
	select {
	case <-f.closedCh:
	default:
		close(f.closedCh)
	}
	return nil
}

// packetRTMPClient returns one keyframe packet, then blocks until Close.
type packetRTMPClient struct {
	mu       sync.Mutex
	sent     bool
	closedCh chan struct{}
}

func newPacketRTMPClient() *packetRTMPClient {
	return &packetRTMPClient{closedCh: make(chan struct{})}
}

func (f *packetRTMPClient) Connect(url string) error { return nil }

func (f *packetRTMPClient) ReadPacket() (av.Packet, error) {
	f.mu.Lock()
	if !f.sent {
		f.sent = true
		f.mu.Unlock()
		return av.Packet{Time: time.Duration(1000), Data: []byte{0x01}, IsKeyFrame: true}, nil
	}
	f.mu.Unlock()
	<-f.closedCh
	return av.Packet{}, errors.New("closed")
}

func (f *packetRTMPClient) Close() error {
	select {
	case <-f.closedCh:
	default:
		close(f.closedCh)
	}
	return nil
}

func waitRemoteActive(t *testing.T, p *HybridStreamPool, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.IsRemoteActive() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("IsRemoteActive() = %v, want %v", p.IsRemoteActive(), want)
}

func waitStreamAlive(t *testing.T, sm *baby.StateManager, uid string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st := sm.GetBabyState(uid); st != nil && st.GetStreamState() == baby.StreamState_Alive {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stream state never became Alive after observed packet")
}

// Actual HybridStreamPool continuity: remote availability stays true while
// the worker runs (synchronous dial + async worker), stall switches source,
// and replacement starts after the worker exits.
func TestHybridPoolActualContinuity(t *testing.T) {
	srv, err := rtmpserver.NewServer(rtmpserver.ServerConfig{AllowedPresets: "localhost"}, baby.NewStateManager())
	if err != nil {
		t.Fatal(err)
	}
	sm := baby.NewStateManager()
	pool := NewHybridStreamPool(HybridStreamPoolConfig{
		BabyUID:      "baby1",
		LocalURL:     "rtmp://127.0.0.1/local/baby1",
		AuthToken:    "test-token",
		StateManager: sm,
		RTMPServer:   srv,
		RTMPClient:   newFakeRTMPClient(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.mu.Lock()
	pool.ctx, pool.cancel = context.WithCancel(ctx)
	pool.started = true
	pool.mu.Unlock()
	srv.GetOrCreateBroadcaster("baby1")

	pool.startRemoteRelay()
	waitRemoteActive(t, pool, true)

	// Local stall with remote available switches to remote.
	pool.mu.Lock()
	pool.activeSource = HybridSourceLocal
	pool.mu.Unlock()
	pool.onStallDetected(HybridSourceLocal)
	if got := pool.GetActiveSource(); got != HybridSourceRemote {
		t.Fatalf("after local stall activeSource = %d, want remote %d", got, HybridSourceRemote)
	}

	// Worker exit allows replacement with a fresh transport.
	pool.mu.RLock()
	old := pool.remoteRelay
	pool.mu.RUnlock()
	if old == nil {
		t.Fatal("remoteRelay nil after start")
	}
	_ = old.Stop()
	pool.mu.Lock()
	pool.remoteActive = false
	pool.mu.Unlock()
	pool.rtmpClient = newFakeRTMPClient()
	pool.startRemoteRelay()
	waitRemoteActive(t, pool, true)

	pool.mu.Lock()
	started := pool.started
	pool.mu.Unlock()
	if !started {
		t.Fatal("pool stopped unexpectedly")
	}
	_ = pool.StopLocalStream(context.Background())
}

// Remote observed media marks Alive; connect alone does not.
func TestHybridPoolRemotePacketMarksAlive(t *testing.T) {
	srv, err := rtmpserver.NewServer(rtmpserver.ServerConfig{AllowedPresets: "localhost"}, baby.NewStateManager())
	if err != nil {
		t.Fatal(err)
	}
	sm := baby.NewStateManager()
	if st := sm.GetBabyState("baby1"); st != nil && st.GetStreamState() == baby.StreamState_Alive {
		t.Fatal("initial state should not be alive")
	}
	pool := NewHybridStreamPool(HybridStreamPoolConfig{
		BabyUID:      "baby1",
		LocalURL:     "rtmp://127.0.0.1/local/baby1",
		AuthToken:    "test-token",
		StateManager: sm,
		RTMPServer:   srv,
		RTMPClient:   newPacketRTMPClient(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.mu.Lock()
	pool.ctx, pool.cancel = context.WithCancel(ctx)
	pool.started = true
	pool.mu.Unlock()
	srv.GetOrCreateBroadcaster("baby1")

	pool.startRemoteRelay()
	waitRemoteActive(t, pool, true)
	waitStreamAlive(t, sm, "baby1")
	_ = pool.StopLocalStream(context.Background())
}
