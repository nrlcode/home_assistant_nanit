package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestSessionConcurrentSave verifies concurrent auth updates and saves are
// serialized, JSON names and Revision are preserved, ordinary saves use 0600,
// and errors are reported (with the donor non-atomic fallback documented).
func TestSessionConcurrentSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.json")

	store, err := InitSessionStore(path)
	if err != nil {
		t.Fatalf("InitSessionStore error = %v", err)
	}
	store.Session.Babies = nil

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			store.UpdateAuth("auth-"+string(rune('a'+n)), "refresh-"+string(rune('a'+n)))
			if err := store.Save(); err != nil {
				t.Errorf("Save error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile error = %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error = %v", err)
	}
	for _, key := range []string{"revision", "authToken", "refreshToken", "authTime", "babies", "lastSeenMessageTime"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("missing JSON key %q (keys=%v)", key, decoded)
		}
	}
	if rev, _ := decoded["revision"].(float64); rev != 3 {
		t.Errorf("revision = %v, want 3", decoded["revision"])
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat error = %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0600 {
		t.Errorf("session file perm = %o, want 600", perm)
	}

	// Reload round-trip preserves revision.
	reloaded, err := InitSessionStore(path)
	if err != nil {
		t.Fatalf("reload InitSessionStore error = %v", err)
	}
	if reloaded.Session.Revision != Revision {
		t.Errorf("reloaded revision = %d, want %d", reloaded.Session.Revision, Revision)
	}

	// Error reporting: saving to a directory path must fail, not panic.
	bad, _ := InitSessionStore(dir)
	bad.Filename = dir
	if err := bad.Save(); err == nil {
		t.Error("Save to directory should return error")
	}
}
