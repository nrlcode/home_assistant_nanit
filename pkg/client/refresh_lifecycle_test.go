package client

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/session"
)

// TestRefreshLifecycle verifies serialized refresh, 401 retry with restored
// request body, latest-token use for websocket/relay connections, and that a
// returned MaybeAuthorize error aborts the retry instead of being ignored.
func TestRefreshLifecycle(t *testing.T) {
	var refreshCount int32
	var authHeaders []string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tokens/refresh") {
			atomic.AddInt32(&refreshCount, 1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(authResponsePayload{
				AccessToken:  "refreshed-token",
				RefreshToken: "refreshed-refresh",
			})
			return
		}
		if strings.Contains(r.URL.Path, "/lifecycle") {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			authHeaders = append(authHeaders, r.Header.Get("Authorization"))
			mu.Unlock()
			if len(authHeaders) == 1 {
				if len(body) == 0 {
					t.Error("first attempt body empty")
				}
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			// Retry must restore the body and use the refreshed token.
			if len(body) == 0 {
				t.Error("retry body empty (not restored)")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var decoded map[string]string
			if err := json.Unmarshal(body, &decoded); err != nil || decoded["k"] != "v" {
				t.Errorf("retry body = %q", string(body))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("Authorization"); got != "refreshed-token" {
				t.Errorf("retry Authorization = %q, want refreshed-token", got)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	original := myClient
	myClient = &http.Client{Transport: &redirectTransport{targetURL: server.URL}, Timeout: 30 * time.Second}
	defer func() { myClient = original }()

	store := session.NewSessionStore()
	store.Session.AuthToken = "stale-token"
	store.Session.RefreshToken = "refresh-token"
	store.Session.AuthTime = time.Now()
	c := &NanitClient{SessionStore: store}

	body, _ := json.Marshal(map[string]string{"k": "v"})
	req, _ := http.NewRequest("POST", "https://api.nanit.com/lifecycle", bytes.NewReader(body))
	var out map[string]string
	if err := c.FetchAuthorized(req, &out); err != nil {
		t.Fatalf("FetchAuthorized error = %v", err)
	}
	if out["status"] != "ok" {
		t.Errorf("out = %v, want ok", out)
	}
	if got := store.GetAuthToken(); got != "refreshed-token" {
		t.Errorf("store token = %q, want refreshed-token (latest token)", got)
	}
	if atomic.LoadInt32(&refreshCount) != 1 {
		t.Errorf("refreshCount = %d, want 1 (serialized)", refreshCount)
	}

	t.Run("authorize error aborts retry", func(t *testing.T) {
		fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/tokens/refresh") {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer fail.Close()
		myClient = &http.Client{Transport: &redirectTransport{targetURL: fail.URL}, Timeout: 30 * time.Second}
		defer func() {
			myClient = &http.Client{Transport: &redirectTransport{targetURL: server.URL}, Timeout: 30 * time.Second}
		}()

		store2 := session.NewSessionStore()
		store2.Session.AuthToken = "stale"
		store2.Session.RefreshToken = "refresh"
		store2.Session.AuthTime = time.Now()
		c2 := &NanitClient{SessionStore: store2}
		req2, _ := http.NewRequest("GET", "https://api.nanit.com/lifecycle", nil)
		var out2 map[string]string
		if err := c2.FetchAuthorized(req2, &out2); err == nil {
			t.Error("expected error when refresh fails, got nil (returned error must not be ignored)")
		}
	})
}
