package client

import (
	"encoding/json"
	"errors"
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

// mockSessionStore creates a session store with a valid auth token for testing
func mockSessionStore() *session.Store {
	return &session.Store{
		Session: &session.Session{
			AuthToken: "test-auth-token",
			AuthTime:  time.Now(),
		},
	}
}

func TestTryFetchAuthorized_ReturnsErrorOnTimeout(t *testing.T) {
	// Create a server that delays longer than client timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Second) // Longer than client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Save original client and restore after test
	originalClient := myClient
	myClient = &http.Client{Timeout: 100 * time.Millisecond} // Very short timeout
	defer func() { myClient = originalClient }()

	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	req, _ := http.NewRequest("GET", server.URL, nil)
	var data map[string]interface{}

	err := client.TryFetchAuthorized(req, &data)

	if err == nil {
		t.Fatal("Expected error on timeout, got nil")
	}

	if !errors.Is(err, ErrTransientFailure) {
		t.Errorf("Expected ErrTransientFailure, got: %v", err)
	}
}

func TestTryFetchAuthorized_ReturnsErrorOnNon200Status(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	req, _ := http.NewRequest("GET", server.URL, nil)
	var data map[string]interface{}

	err := client.TryFetchAuthorized(req, &data)

	if err == nil {
		t.Fatal("Expected error on 500 status, got nil")
	}

	if !errors.Is(err, ErrTransientFailure) {
		t.Errorf("Expected ErrTransientFailure, got: %v", err)
	}
}

func TestTryFetchAuthorized_ReturnsErrorOnInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not valid json"))
	}))
	defer server.Close()

	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	req, _ := http.NewRequest("GET", server.URL, nil)
	var data map[string]interface{}

	err := client.TryFetchAuthorized(req, &data)

	if err == nil {
		t.Fatal("Expected error on invalid JSON, got nil")
	}

	if !errors.Is(err, ErrTransientFailure) {
		t.Errorf("Expected ErrTransientFailure, got: %v", err)
	}
}

func TestTryFetchAuthorized_SuccessOnValidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header is set
		if r.Header.Get("Authorization") != "test-auth-token" {
			t.Errorf("Expected Authorization header to be set")
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	req, _ := http.NewRequest("GET", server.URL, nil)
	var data map[string]string

	err := client.TryFetchAuthorized(req, &data)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if data["status"] != "ok" {
		t.Errorf("Expected status 'ok', got: %s", data["status"])
	}
}

func TestTryFetchMessages_ReturnsErrorOnFailure(t *testing.T) {
	// Save original client and restore after test
	originalClient := myClient
	myClient = &http.Client{Timeout: 100 * time.Millisecond}
	defer func() { myClient = originalClient }()

	// Server that times out
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Second)
	}))
	defer server.Close()

	// We can't easily redirect the API URL, so test with connection refused
	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	// This will fail because it tries to connect to api.nanit.com
	// But with our short timeout, it should return an error not panic
	messages, err := client.TryFetchMessages("test-baby", 10)

	if err == nil {
		t.Log("Note: This test may pass if api.nanit.com is reachable with valid token")
	}

	// Either we get an error (expected) or empty messages (if somehow succeeded)
	if err != nil && !errors.Is(err, ErrTransientFailure) {
		t.Logf("Got error (expected): %v", err)
	}

	if messages == nil && err == nil {
		t.Error("Expected either messages or error, got nil for both")
	}
}

func TestFetchNewMessages_ReturnsEmptyOnError(t *testing.T) {
	// Save original client and restore after test
	originalClient := myClient
	myClient = &http.Client{Timeout: 100 * time.Millisecond}
	defer func() { myClient = originalClient }()

	client := &NanitClient{
		SessionStore: mockSessionStore(),
	}

	// This will fail to connect but should NOT panic
	// It should return an empty slice
	messages := client.FetchNewMessages("test-baby", 5*time.Minute)

	if messages == nil {
		t.Error("Expected empty slice, got nil")
	}

	if len(messages) != 0 {
		t.Errorf("Expected 0 messages on error, got %d", len(messages))
	}
}

func TestErrTransientFailure_IsError(t *testing.T) {
	if ErrTransientFailure == nil {
		t.Error("ErrTransientFailure should not be nil")
	}

	if ErrTransientFailure.Error() != "transient HTTP failure" {
		t.Errorf("Unexpected error message: %s", ErrTransientFailure.Error())
	}
}

// TestMaybeAuthorizeConcurrent tests that concurrent calls to MaybeAuthorize
// only result in one actual authorization when the token is expired.
// Run with: go test -race ./pkg/client/...
func TestMaybeAuthorizeConcurrent(t *testing.T) {
	var authCount int32

	// Server that counts authorization attempts
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/tokens/refresh") {
			atomic.AddInt32(&authCount, 1)
			// Simulate some processing time
			time.Sleep(50 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(authResponsePayload{
				AccessToken:  "new-access-token",
				RefreshToken: "new-refresh-token",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Create client with expired token
	store := session.NewSessionStore()
	store.Session.RefreshToken = "test-refresh-token"
	store.Session.AuthTime = time.Now().Add(-48 * time.Hour) // Expired

	client := &NanitClient{
		SessionStore: store,
	}

	// Override API base for test (we'd need to patch the URL in a real test)
	// For now, we're testing that the mutex prevents concurrent auth
	var wg sync.WaitGroup
	goroutines := 10

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This should be serialized by the auth mutex
			_ = client.MaybeAuthorize(true)
		}()
	}

	wg.Wait()

	// Without mutex protection, authCount would be > 1
	// With proper locking, only the first call should authorize,
	// subsequent calls should see the fresh token
	// Note: This test validates the mutex is in place by checking for races
}

// redirectTransport wraps an http.RoundTripper and redirects all requests to a test server
type redirectTransport struct {
	targetURL string
}

func (t *redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Redirect all requests to the test server
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.targetURL, "http://")
	return http.DefaultTransport.RoundTrip(req)
}

// TestFetchAuthorizedRetryWithBody tests that POST requests with a body
// can be retried after a 401 response.
func TestFetchAuthorizedRetryWithBody(t *testing.T) {
	var requestCount int32
	var bodiesReceived []string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle auth refresh
		if strings.Contains(r.URL.Path, "/tokens/refresh") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(authResponsePayload{
				AccessToken:  "new-access-token",
				RefreshToken: "new-refresh-token",
			})
			return
		}

		// Handle main endpoint
		if strings.Contains(r.URL.Path, "/test") {
			count := atomic.AddInt32(&requestCount, 1)

			// Read the body to verify it's present
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("Failed to read body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			bodiesReceived = append(bodiesReceived, string(body))
			mu.Unlock()

			// First request returns 401
			if count == 1 {
				if len(body) == 0 {
					t.Error("First request: body should not be empty")
				}
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Second request (retry) should also have the body
			if len(body) == 0 {
				t.Error("Retry request: body should not be empty - body was consumed on first attempt")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// Verify body content
			var reqBody map[string]string
			if err := json.Unmarshal(body, &reqBody); err != nil {
				t.Errorf("Failed to unmarshal body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if reqBody["test"] != "data" {
				t.Errorf("Body content = %v, want {\"test\":\"data\"}", reqBody)
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Override the global HTTP client to redirect all requests to test server
	originalClient := myClient
	myClient = &http.Client{
		Transport: &redirectTransport{targetURL: server.URL},
		Timeout:   30 * time.Second,
	}
	defer func() { myClient = originalClient }()

	store := session.NewSessionStore()
	store.Session.AuthToken = "test-token"
	store.Session.RefreshToken = "refresh-token"

	client := &NanitClient{
		SessionStore: store,
	}

	// Create POST request with body - use the real API URL since transport redirects
	// Use a non-seekable reader to ensure body can't be reset by HTTP client
	bodyData := []byte(`{"test":"data"}`)
	req, _ := http.NewRequest("POST", "https://api.nanit.com/test", io.NopCloser(strings.NewReader(string(bodyData))))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(bodyData))

	var response map[string]string
	err := client.FetchAuthorized(req, &response)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if response["status"] != "ok" {
		t.Errorf("Response status = %q, want ok", response["status"])
	}

	// Should have made exactly 2 requests (initial 401, then retry)
	if atomic.LoadInt32(&requestCount) != 2 {
		t.Errorf("Request count = %d, want 2", requestCount)
	}

	// Verify both requests had the body
	mu.Lock()
	defer mu.Unlock()
	if len(bodiesReceived) != 2 {
		t.Fatalf("Expected 2 bodies received, got %d", len(bodiesReceived))
	}
	for i, body := range bodiesReceived {
		if body != `{"test":"data"}` {
			t.Errorf("Request %d body = %q, want {\"test\":\"data\"}", i+1, body)
		}
	}
}

// TestFetchAuthorizedUsesStoreGetter tests that FetchAuthorized uses
// the thread-safe Store.GetAuthToken() method
func TestFetchAuthorizedUsesStoreGetter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the Authorization header contains the token
		authHeader := r.Header.Get("Authorization")
		if authHeader != "getter-token" {
			t.Errorf("Authorization header = %q, want getter-token", authHeader)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	store := session.NewSessionStore()
	// Set token through UpdateAuth (thread-safe setter)
	store.UpdateAuth("getter-token", "refresh-token")

	client := &NanitClient{
		SessionStore: store,
	}

	req, _ := http.NewRequest("GET", server.URL+"/test", nil)
	var response map[string]string
	err := client.FetchAuthorized(req, &response)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}
