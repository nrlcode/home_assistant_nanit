package client

import (
	"context"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"testing"
	"time"
)

func TestAuditorPollingCancelWhileAuthorizationBusy(t *testing.T) {
	c := &NanitClient{SessionStore: session.NewSessionStore()}
	// Another real authorization owner (websocket/relay) can hold this lock
	// while its contextless HTTP request is in flight.
	c.authMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.TryFetchMessagesCtx(ctx, "baby1", 10); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
		c.authMu.Unlock()
	case <-time.After(100 * time.Millisecond):
		c.authMu.Unlock()
		<-done
		t.Fatal("poll cancellation waits for unrelated authorization mutex owner")
	}
}
