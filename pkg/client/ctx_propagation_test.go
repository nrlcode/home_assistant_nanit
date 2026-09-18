package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
)

// newCtxTestClient redirects all api.nanit.com traffic to srv and returns a
// NanitClient with a fresh token. Caller must restore myClient via cleanup.
func newCtxTestClient(t *testing.T, srv *httptest.Server, token, refresh string) *NanitClient {
	t.Helper()
	original := myClient
	myClient = &http.Client{Transport: &redirectTransport{targetURL: srv.URL}, Timeout: 30 * time.Second}
	t.Cleanup(func() { myClient = original })
	store := session.NewSessionStore()
	store.Session.AuthToken = token
	store.Session.RefreshToken = refresh
	store.Session.AuthTime = time.Now()
	return &NanitClient{SessionStore: store}
}

func TestTryFetchMessagesCtxPropagates500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := newCtxTestClient(t, srv, "tok", "refresh")
	_, err := c.TryFetchMessagesCtx(context.Background(), "baby1", 10)
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
}

func TestTryFetchMessagesCtxCancellationDuringGET(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/messages") {
			select {
			case <-r.Context().Done():
				return
			case <-release:
			}
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(messagesResponsePayload{})
	}))
	defer srv.Close()
	defer close(release)
	c := newCtxTestClient(t, srv, "tok", "refresh")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.TryFetchMessagesCtx(ctx, "baby1", 10)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil || (err != context.Canceled && !strings.Contains(err.Error(), "canceled")) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled GET did not return")
	}
}

func TestTryFetchMessagesCtxCancellationDuringRefresh(t *testing.T) {
	refreshEntered := make(chan struct{}, 1)
	stopRefresh := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/tokens/refresh"):
			select {
			case refreshEntered <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				return
			case <-stopRefresh:
				return
			case <-time.After(5 * time.Second):
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(authResponsePayload{AccessToken: "new", RefreshToken: "new-r"})
		case strings.Contains(r.URL.Path, "/messages"):
			w.WriteHeader(http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	defer close(stopRefresh)
	c := newCtxTestClient(t, srv, "stale", "refresh")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := c.TryFetchMessagesCtx(ctx, "baby1", 10)
		done <- err
	}()
	select {
	case <-refreshEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh not entered")
	}
	cancel()
	select {
	case err := <-done:
		if elapsed := time.Since(start); elapsed > 4*time.Second {
			t.Fatalf("refresh ignored cancellation: took %v, want fast abort", elapsed)
		}
		if err == nil || (err != context.Canceled && err != context.DeadlineExceeded && !strings.Contains(err.Error(), "canceled")) {
			t.Fatalf("err = %v, want context.Canceled during refresh", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("cancelled refresh did not return")
	}
}

func TestTryFetchMessagesCtxAbsentTokenCancellation(t *testing.T) {
	refreshEntered := make(chan struct{}, 1)
	stopRefresh := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tokens/refresh") {
			select {
			case refreshEntered <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				return
			case <-stopRefresh:
				return
			case <-time.After(5 * time.Second):
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(authResponsePayload{AccessToken: "new", RefreshToken: "new-r"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	defer close(stopRefresh)
	c := newCtxTestClient(t, srv, "", "")
	// Seed refresh fallback so absent-token path enters refresh.
	c.RefreshToken = "fallback-refresh"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := c.TryFetchMessagesCtx(ctx, "baby1", 10)
		done <- err
	}()
	select {
	case <-refreshEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("absent-token refresh not entered")
	}
	cancel()
	select {
	case err := <-done:
		if elapsed := time.Since(start); elapsed > 4*time.Second {
			t.Fatalf("absent-token refresh ignored cancellation: took %v", elapsed)
		}
		if err == nil || (err != context.Canceled && !strings.Contains(err.Error(), "canceled")) {
			t.Fatalf("err = %v, want context.Canceled for absent token", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("absent-token cancel did not return")
	}
}
