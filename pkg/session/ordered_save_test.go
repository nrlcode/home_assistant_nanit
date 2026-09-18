package session

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOrderedSaveRetainsNewest(t *testing.T) {
	store, err := InitSessionStore(filepath.Join(t.TempDir(), "session.json"))
	if err != nil {
		t.Fatal(err)
	}

	firstSnapshotted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	store.afterSnapshot = func() {
		once.Do(func() {
			close(firstSnapshotted)
			<-releaseFirst
		})
	}

	store.UpdateAuth("auth-old", "refresh-old")
	firstDone := make(chan error, 1)
	go func() { firstDone <- store.Save() }()
	<-firstSnapshotted

	store.UpdateAuth("auth-new", "refresh-new")
	secondDone := make(chan error, 1)
	go func() { secondDone <- store.Save() }()
	close(releaseFirst)

	for i, done := range []chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Save %d error = %v", i+1, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Save %d did not return", i+1)
		}
	}

	reloaded, err := InitSessionStore(store.Filename)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Session.AuthToken; got != "auth-new" {
		t.Fatalf("persisted authToken = %q, want auth-new (stale snapshot won)", got)
	}
	if got := reloaded.Session.RefreshToken; got != "refresh-new" {
		t.Fatalf("persisted refreshToken = %q, want refresh-new", got)
	}
}
