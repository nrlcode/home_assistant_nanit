package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
)

type auditBlockedDial struct {
	entered chan struct{}
	release chan struct{}
}

func (c *auditBlockedDial) Connect(string) error {
	close(c.entered)
	<-c.release
	return nil
}
func (c *auditBlockedDial) ReadPacket() (av.Packet, error) { return av.Packet{}, errors.New("closed") }
func (c *auditBlockedDial) Close() error                   { return nil }

func TestAuditorStopJoinsInitialConnect(t *testing.T) {
	c := &auditBlockedDial{entered: make(chan struct{}), release: make(chan struct{})}
	connected := make(chan struct{}, 1)
	r := NewRemoteRelay(RemoteRelayConfig{
		BabyUID:    "baby1",
		AuthToken:  "synthetic",
		RTMPClient: c,
		OnConnected: func() {
			connected <- struct{}{}
		},
	})
	started := make(chan struct{})
	go func() {
		_ = r.Start(context.Background())
		close(started)
	}()
	<-c.entered
	stopped := make(chan struct{})
	go func() {
		_ = r.Stop()
		close(stopped)
	}()
	early := false
	select {
	case <-stopped:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	close(c.release)
	<-started
	<-stopped
	if early {
		t.Error("Stop returned while initial Connect was still in flight")
	}
	select {
	case <-connected:
		t.Error("OnConnected ran after Stop began")
	default:
	}
}
