package app

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/mqtt"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

// TestAppNotificationReadinessAndManagerOnEvent verifies synchronous client
// ownership and the real readiness->Manager->MQTT funnel without test-side
// EnsureClient or direct PublishMotionSoundEvent calls.
func TestAppNotificationReadinessAndManagerOnEvent(t *testing.T) {
	prevTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/tokens/refresh") {
			body := `{"access_token":"test-token","refresh_token":"test-refresh"}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}
		if strings.Contains(r.URL.Path, "/login") {
			body := `{"access_token":"test-token","refresh_token":"test-refresh"}`
			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}
		now := time.Now().Unix()
		body := `{"messages":[{"id":42,"baby_uid":"baby1","type":"MOTION","time":` + itoa(now) + `}]}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = prevTransport })

	dir := t.TempDir()
	opts := Opts{
		SessionFile: filepath.Join(dir, "session.json"),
		MQTT: &mqtt.Opts{
			BrokerURL:   "tcp://127.0.0.1:1883",
			TopicPrefix: "nanit",
		},
		EventPolling: EventPollingOpts{
			Enabled:         true,
			PollingInterval: 10 * time.Millisecond,
			MessageTimeout:  5 * time.Minute,
		},
	}
	a, err := NewApp(opts)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	a.SessionStore.UpdateBabies([]baby.Baby{{UID: "baby1", Name: "B"}})

	// Production readiness must create the client; the test never calls
	// EnsureClient before startNotificationPolling.
	if got := a.MQTTConnection.GetClient(); got != nil {
		t.Fatalf("GetClient before polling = non-nil, want nil (production must own readiness)")
	}
	done := make(chan struct{})
	a.MQTTConnection.OnEvent = func(babyUID, key string, _ int32) {
		if babyUID == "baby1" && key == "motion" {
			select {
			case <-done:
			default:
				close(done)
			}
		}
	}
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		a.startNotificationPolling(ctx)
		if a.notificationManager == nil {
			t.Error("notificationManager nil after startNotificationPolling")
			return
		}
		if got := a.MQTTConnection.GetClient(); got == nil {
			t.Error("production readiness must EnsureClient (got nil)")
		}
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		runner.Cancel()
		t.Fatal("Manager OnEvent funnel did not reach MQTT publish (real REST path)")
	}
	runner.Cancel()
}

func TestAppEnsureClientRace(t *testing.T) {
	dir := t.TempDir()
	a, err := NewApp(Opts{
		SessionFile: filepath.Join(dir, "session.json"),
		MQTT:        &mqtt.Opts{BrokerURL: "tcp://127.0.0.1:1883", TopicPrefix: "nanit"},
	})
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c := a.MQTTConnection.EnsureClient(); c == nil {
				t.Error("concurrent EnsureClient returned nil")
			}
			if c := a.MQTTConnection.GetClient(); c == nil {
				t.Error("concurrent GetClient returned nil after EnsureClient")
			}
		}()
	}
	wg.Wait()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [32]byte
	pos := len(b)
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
