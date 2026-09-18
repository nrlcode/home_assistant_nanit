package client

import (
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/session"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
)

func TestWebsocketBinaryMessageNilReadyState(t *testing.T) {
	// Test that receiving a binary message before connection is ready
	// does not cause a nil pointer panic

	store := session.NewSessionStore()
	babyStateManager := baby.NewStateManager()
	api := &NanitClient{SessionStore: store}

	manager := &WebsocketConnectionManager{
		BabyUID:          "test-baby",
		CameraUID:        "test-camera",
		Session:          store.Session,
		API:              api,
		BabyStateManager: babyStateManager,
		// readyState is nil (not connected yet)
	}

	// Simulate receiving a message - this should not panic
	// We'll call the internal logic that would be triggered by OnBinaryMessage

	// First verify readyState is nil
	manager.mu.RLock()
	readyState := manager.readyState
	manager.mu.RUnlock()

	if readyState != nil {
		t.Fatal("Expected readyState to be nil before connection")
	}

	// The handleBinaryMessage function should safely handle nil readyState
	// This tests the fix - if unfixed, this would panic
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("handleBinaryMessage panicked with nil readyState: %v", r)
			}
			done <- true
		}()
		manager.handleBinaryMessage([]byte{})
	}()

	select {
	case <-done:
		// Good - function completed without panic
	case <-time.After(time.Second):
		t.Fatal("handleBinaryMessage timed out")
	}
}

func TestWebsocketHandlerRegistration(t *testing.T) {
	// Test that handlers can be registered before connection is ready
	store := session.NewSessionStore()
	babyStateManager := baby.NewStateManager()
	api := &NanitClient{SessionStore: store}

	manager := &WebsocketConnectionManager{
		BabyUID:          "test-baby",
		CameraUID:        "test-camera",
		Session:          store.Session,
		API:              api,
		BabyStateManager: babyStateManager,
	}

	var handlerCalled bool
	var mu sync.Mutex

	manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
		mu.Lock()
		handlerCalled = true
		mu.Unlock()
	})

	// Handler should be registered
	manager.mu.RLock()
	numSubscribers := len(manager.readySubscribers)
	manager.mu.RUnlock()

	if numSubscribers != 1 {
		t.Errorf("Expected 1 subscriber, got %d", numSubscribers)
	}

	// Handler should not be called yet (no ready state)
	mu.Lock()
	called := handlerCalled
	mu.Unlock()

	if called {
		t.Error("Handler should not be called before connection is ready")
	}
}
