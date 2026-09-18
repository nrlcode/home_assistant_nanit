package notification

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/client"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestManagerBackoffCapAndRecoveryViaREST drives Manager backoff through the
// real NanitClient REST path (not a fake fetcher): 500s must increase backoff
// monotonically to the configured cap, and a subsequent successful fetch must
// reset it.
func TestManagerBackoffCapAndRecoveryViaREST(t *testing.T) {
	var mode atomic.Int32 // 0=fail 500, 1=succeed
	prevTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if mode.Load() == 0 {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("boom")),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}
		now := time.Now().Unix()
		body := fmt.Sprintf(`{"messages":[{"id":7,"baby_uid":"baby1","type":"MOTION","time":%d}]}`, now)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = prevTransport })

	store := session.NewSessionStore()
	store.Session.AuthToken = "tok"
	store.Session.RefreshToken = "refresh"
	store.Session.AuthTime = time.Now()
	restClient := &client.NanitClient{SessionStore: store}

	cfg := PollerConfig{
		PollInterval: 100 * time.Millisecond, Jitter: 0,
		MaxBackoff: 5 * time.Minute, MessageTimeout: 5 * time.Minute, DedupMaxSize: 100,
	}
	mgr := NewManager(ManagerConfig{
		PollerConfig: cfg,
		TopicPrefix:  "nanit",
		Babies:       []baby.Baby{{UID: "baby1", Name: "B"}},
	}, restClient, NewMockMQTTClient())
	ctx := context.Background()
	var last time.Duration
	capped := false
	for i := 0; i < 20; i++ {
		if err := mgr.pollBaby(ctx, baby.Baby{UID: "baby1", Name: "B"}); err == nil {
			t.Fatalf("pollBaby %d should fail on 500", i)
		}
		got := mgr.poller.CurrentBackoff()
		if i > 0 && got < last {
			t.Fatalf("backoff decreased: %v -> %v", last, got)
		}
		last = got
		if got == 5*time.Minute {
			capped = true
			break
		}
	}
	if !capped {
		t.Fatalf("backoff never reached cap 5m, last=%v", last)
	}
	mode.Store(1)
	if err := mgr.pollBaby(ctx, baby.Baby{UID: "baby1", Name: "B"}); err != nil {
		t.Fatalf("recovery pollBaby: %v", err)
	}
	if got := mgr.poller.CurrentBackoff(); got != 0 {
		t.Fatalf("after recovery backoff = %v, want 0", got)
	}
}
