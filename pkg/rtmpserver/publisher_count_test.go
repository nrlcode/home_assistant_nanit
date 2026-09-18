package rtmpserver

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

// Named/suffixed publisher accounting: two slots share babyUID count,
// removing one leaves 1, removing the last leaves 0 so failover fires.
func TestPublisherNamedAndUnsuffixedAccounting(t *testing.T) {
	h := newRtmpHandler(baby.NewStateManager())

	h.registerLocalPublisher("baby1", "_0", "baby1_0")
	h.registerLocalPublisher("baby1", "_1", "baby1_1")
	if !h.IsLocalPublisherActive("baby1") {
		t.Fatal("baby1 should be active with two slots")
	}
	if remaining := h.unregisterLocalPublisher("baby1", "baby1_0"); remaining != 1 {
		t.Fatalf("after one slot removed: remaining=%d, want 1", remaining)
	}
	if !h.IsLocalPublisherActive("baby1") {
		t.Fatal("baby1 should stay active with one slot left")
	}
	if remaining := h.unregisterLocalPublisher("baby1", "baby1_1"); remaining != 0 {
		t.Fatalf("after last slot removed: remaining=%d, want 0", remaining)
	}
	if h.IsLocalPublisherActive("baby1") {
		t.Fatal("baby1 should be inactive after last publisher removed")
	}
}
