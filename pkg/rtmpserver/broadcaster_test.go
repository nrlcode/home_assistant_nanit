package rtmpserver

import (
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/stretchr/testify/assert"
)

// ============================================================
// REGRESSION TESTS FOR RTMP BROADCASTER
// These tests ensure existing functionality works before we
// make changes for stream failover implementation.
// ============================================================

func TestNewBroadcaster(t *testing.T) {
	b := newBroadcaster()
	assert.NotNil(t, b)
}

func TestBroadcasterNewSubscriber(t *testing.T) {
	b := newBroadcaster()
	sub := b.newSubscriber()

	assert.NotNil(t, sub)
	assert.False(t, sub.initialized)
	assert.NotNil(t, sub.pktC)
}

func TestBroadcasterMultipleSubscribers(t *testing.T) {
	b := newBroadcaster()
	sub1 := b.newSubscriber()
	sub2 := b.newSubscriber()
	sub3 := b.newSubscriber()

	assert.NotNil(t, sub1)
	assert.NotNil(t, sub2)
	assert.NotNil(t, sub3)

	// Each should have their own channel
	assert.NotEqual(t, sub1.pktC, sub2.pktC)
	assert.NotEqual(t, sub2.pktC, sub3.pktC)
}

func TestBroadcasterUnsubscribe(t *testing.T) {
	b := newBroadcaster()
	sub := b.newSubscriber()

	// Unsubscribe
	b.unsubscribe(sub)

	// Broadcasting should not panic with no subscribers
	pkt := av.Packet{Type: 1} // Audio/Video packet
	b.broadcast(pkt)
}

func TestBroadcasterBroadcastToSubscriber(t *testing.T) {
	b := newBroadcaster()
	sub := b.newSubscriber()

	// Send audio/video packet (Type <= 2)
	pkt := av.Packet{Type: 1, Data: []byte{0x01, 0x02}}
	b.broadcast(pkt)

	// Subscriber should receive packet
	select {
	case received := <-sub.pktC:
		assert.Equal(t, pkt.Type, received.Type)
		assert.Equal(t, pkt.Data, received.Data)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for packet")
	}
}

func TestBroadcasterBroadcastToMultipleSubscribers(t *testing.T) {
	b := newBroadcaster()
	sub1 := b.newSubscriber()
	sub2 := b.newSubscriber()

	// Send audio/video packet
	pkt := av.Packet{Type: 1, Data: []byte{0x01}}
	b.broadcast(pkt)

	// Both subscribers should receive packet
	select {
	case received := <-sub1.pktC:
		assert.Equal(t, pkt.Type, received.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for packet on sub1")
	}

	select {
	case received := <-sub2.pktC:
		assert.Equal(t, pkt.Type, received.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for packet on sub2")
	}
}

func TestBroadcasterHeaderPackets(t *testing.T) {
	b := newBroadcaster()

	// Send header packets (Type > 2)
	headerPkt1 := av.Packet{Type: 3, Data: []byte{0xAA}}
	headerPkt2 := av.Packet{Type: 4, Data: []byte{0xBB}}
	b.broadcast(headerPkt1)
	b.broadcast(headerPkt2)

	// Now add subscriber
	sub := b.newSubscriber()

	// Send audio/video packet - this should trigger header packets first
	videoPkt := av.Packet{Type: 1, Data: []byte{0x01}}
	b.broadcast(videoPkt)

	// Subscriber should receive header packets before video packet
	select {
	case received := <-sub.pktC:
		assert.Equal(t, headerPkt1.Type, received.Type)
		assert.Equal(t, headerPkt1.Data, received.Data)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for header packet 1")
	}

	select {
	case received := <-sub.pktC:
		assert.Equal(t, headerPkt2.Type, received.Type)
		assert.Equal(t, headerPkt2.Data, received.Data)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for header packet 2")
	}

	select {
	case received := <-sub.pktC:
		assert.Equal(t, videoPkt.Type, received.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for video packet")
	}
}

func TestBroadcasterCloseSubscribers(t *testing.T) {
	b := newBroadcaster()
	sub1 := b.newSubscriber()
	sub2 := b.newSubscriber()

	// Mark as initialized so we don't send header packets
	sub1.initialized = true
	sub2.initialized = true

	// Close all subscribers
	b.closeSubscribers()

	// Channels should be closed
	_, open1 := <-sub1.pktC
	_, open2 := <-sub2.pktC

	assert.False(t, open1)
	assert.False(t, open2)
}

func TestBroadcasterSubscriberInitialization(t *testing.T) {
	b := newBroadcaster()

	// Add header packet
	headerPkt := av.Packet{Type: 5, Data: []byte{0xFF}}
	b.broadcast(headerPkt)

	// Add subscriber
	sub := b.newSubscriber()
	assert.False(t, sub.initialized)

	// First video packet should trigger initialization
	videoPkt := av.Packet{Type: 1}
	b.broadcast(videoPkt)

	// Now subscriber should be initialized
	// (After receiving the broadcast, initialized is set to true)
	// Drain header packet
	<-sub.pktC
	// Drain video packet
	<-sub.pktC

	assert.True(t, sub.initialized)
}

// TestBroadcasterNoSubscribers ensures broadcast doesn't panic with no subscribers
func TestBroadcasterNoSubscribers(t *testing.T) {
	b := newBroadcaster()

	// Should not panic
	pkt := av.Packet{Type: 1, Data: []byte{0x01}}
	b.broadcast(pkt)
}

// TestBroadcasterUnsubscribeNonExistent ensures unsubscribing non-existent subscriber doesn't panic
func TestBroadcasterUnsubscribeNonExistent(t *testing.T) {
	b := newBroadcaster()
	fakeSub := &subscriber{}

	// Should not panic
	b.unsubscribe(fakeSub)
}
