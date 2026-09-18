package session

import (
	"sync"
	"testing"
	"time"
)

func TestSessionConcurrentTokenAccess(t *testing.T) {
	// Test that concurrent reads/writes don't cause race conditions
	// Run with: go test -race ./pkg/session/...
	store := NewSessionStore()

	var wg sync.WaitGroup
	iterations := 100

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			store.UpdateAuth("token-"+string(rune(i)), "refresh-"+string(rune(i)))
		}
	}()

	// Reader goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = store.GetAuthToken()
				_ = store.GetRefreshToken()
			}
		}()
	}

	wg.Wait()
}

func TestStoreGetAuthToken(t *testing.T) {
	store := NewSessionStore()
	store.Session.AuthToken = "test-auth-token"

	got := store.GetAuthToken()
	if got != "test-auth-token" {
		t.Errorf("GetAuthToken() = %q, want %q", got, "test-auth-token")
	}
}

func TestStoreGetAuthTokenEmpty(t *testing.T) {
	store := NewSessionStore()

	got := store.GetAuthToken()
	if got != "" {
		t.Errorf("GetAuthToken() = %q, want empty string", got)
	}
}

func TestStoreGetRefreshToken(t *testing.T) {
	store := NewSessionStore()
	store.Session.RefreshToken = "test-refresh-token"

	got := store.GetRefreshToken()
	if got != "test-refresh-token" {
		t.Errorf("GetRefreshToken() = %q, want %q", got, "test-refresh-token")
	}
}

func TestStoreUpdateAuth(t *testing.T) {
	store := NewSessionStore()

	beforeUpdate := time.Now()
	store.UpdateAuth("new-auth-token", "new-refresh-token")
	afterUpdate := time.Now()

	if store.Session.AuthToken != "new-auth-token" {
		t.Errorf("AuthToken = %q, want %q", store.Session.AuthToken, "new-auth-token")
	}
	if store.Session.RefreshToken != "new-refresh-token" {
		t.Errorf("RefreshToken = %q, want %q", store.Session.RefreshToken, "new-refresh-token")
	}
	if store.Session.AuthTime.Before(beforeUpdate) || store.Session.AuthTime.After(afterUpdate) {
		t.Errorf("AuthTime = %v, want between %v and %v", store.Session.AuthTime, beforeUpdate, afterUpdate)
	}
}

func TestStoreUpdateAuthSetsTimestamp(t *testing.T) {
	store := NewSessionStore()

	// AuthTime should be zero initially
	if !store.Session.AuthTime.IsZero() {
		t.Errorf("Initial AuthTime should be zero, got %v", store.Session.AuthTime)
	}

	store.UpdateAuth("token", "refresh")

	// AuthTime should now be set
	if store.Session.AuthTime.IsZero() {
		t.Error("AuthTime should be set after UpdateAuth")
	}
}
