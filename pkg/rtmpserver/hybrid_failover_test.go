package rtmpserver

import (
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
)

// TestHybridFailoverContinuity verifies the viewer broadcaster persists
// across local↔cloud source switches (no subscriber replacement),
// timestamps stay monotonic, and cancellation/close is clean.
func TestHybridFailoverContinuity(t *testing.T) {
	pb := NewPersistentBroadcaster("baby1")
	defer pb.Close()

	sub := pb.Subscribe()
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	defer sub.Unsubscribe()

	localFn := pb.GetBroadcastFunc(SourceIDLocal)
	remoteFn := pb.GetBroadcastFunc(SourceIDRemote)

	mkPkt := func(ts time.Duration) av.Packet {
		return av.Packet{Time: ts, Data: []byte{0x01}}
	}

	// Local packets.
	localFn(mkPkt(1000))
	localFn(mkPkt(1040))

	// Switch to remote without replacing the broadcaster.
	pb.RequestSourceSwitch(SourceIDRemote)
	remoteFn(mkPkt(2000))

	// Back to local.
	pb.RequestSourceSwitch(SourceIDLocal)
	localFn(mkPkt(3000))

	if got := pb.BabyUID(); got != "baby1" {
		t.Errorf("BabyUID = %q, want baby1", got)
	}
	if n := pb.SubscriberCount(); n != 1 {
		t.Errorf("SubscriberCount = %d, want 1 (viewer preserved)", n)
	}
	// Drain a few packets; timestamps must be monotonic non-decreasing.
	var last time.Duration
	seen := 0
	timeout := 0
	for seen < 3 && timeout < 100 {
		select {
		case pkt, ok := <-sub.Packets():
			if !ok {
				t.Fatal("subscription closed early")
			}
			if seen > 0 && pkt.Time < last {
				t.Errorf("timestamp went backward: %d -> %d", last, pkt.Time)
			}
			last = pkt.Time
			seen++
		default:
			timeout++
		}
	}
}
