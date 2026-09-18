package rtmpserver

import (
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/notedit/rtmp/format/rtmp"
	"net"
	"testing"
)

type auditConn struct {
	net.Conn
	closed bool
}

func (c *auditConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 1234}
}
func (c *auditConn) Close() error { c.closed = true; return nil }

func TestAuditorEmptyACLRejectsAtGate(t *testing.T) {
	for _, publishing := range []bool{false, true} {
		t.Run(map[bool]string{false: "play", true: "publish"}[publishing], func(t *testing.T) {
			s, err := NewServer(ServerConfig{}, baby.NewStateManager())
			if err != nil {
				t.Fatal(err)
			}
			n := &auditConn{}
			// URL is a sentinel: a denied peer must be rejected before normal routing reads it.
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("empty ACL entered normal routing instead of rejecting peer: %v", r)
				}
			}()
			s.handleConnectionWithSecurity(&rtmp.Conn{Publishing: publishing}, n)
			if !n.closed {
				t.Error("empty ACL did not close denied peer")
			}
		})
	}
}

func TestAuditorSinglePublisherRemovalCount(t *testing.T) {
	h := newRtmpHandler(baby.NewStateManager())
	h.registerLocalPublisher("baby1", "", "baby1")
	if remaining := h.unregisterLocalPublisher("baby1", "baby1"); remaining != 0 {
		t.Fatalf("last publisher removed: remaining=%d, want 0 so unhealthy/failover fires", remaining)
	}
}

func TestAuditorServerCloseClosesViewer(t *testing.T) {
	s, _ := NewServer(ServerConfig{AllowedPresets: "localhost"}, baby.NewStateManager())
	s.GetOrCreateBroadcaster("baby1")
	sub := s.GetBroadcaster("baby1").Subscribe()
	defer sub.Unsubscribe()
	s.Close()
	select {
	case _, ok := <-sub.Packets():
		if ok {
			t.Error("unexpected packet")
		}
	default:
		t.Error("server Close left persistent viewer subscription open")
	}
}
