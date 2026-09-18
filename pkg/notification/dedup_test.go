package notification

import (
	"sync"
	"testing"
)

func TestDeduplicatorNewMessagesNotSeen(t *testing.T) {
	d := NewDeduplicator(100)

	if d.IsSeen("baby1", 1) {
		t.Error("New message 1 should not be seen")
	}
	if d.IsSeen("baby1", 2) {
		t.Error("New message 2 should not be seen")
	}
	if d.IsSeen("baby1", 3) {
		t.Error("New message 3 should not be seen")
	}
}

func TestDeduplicatorMarkSeen(t *testing.T) {
	d := NewDeduplicator(100)

	// Initially not seen
	if d.IsSeen("baby1", 42) {
		t.Error("Message should not be seen before marking")
	}

	// Mark as seen
	d.MarkSeen("baby1", 42)

	// Now should be seen
	if !d.IsSeen("baby1", 42) {
		t.Error("Message should be seen after marking")
	}
}

func TestDeduplicatorMultipleMarks(t *testing.T) {
	d := NewDeduplicator(100)

	ids := []int{1, 2, 3, 4, 5}

	// Mark all
	for _, id := range ids {
		d.MarkSeen("baby1", id)
	}

	// Verify all seen
	for _, id := range ids {
		if !d.IsSeen("baby1", id) {
			t.Errorf("Message %d should be seen", id)
		}
	}

	// Verify unknown not seen
	if d.IsSeen("baby1", 999) {
		t.Error("Message 999 should not be seen")
	}
}

func TestDeduplicatorBabyScoped(t *testing.T) {
	d := NewDeduplicator(100)

	d.MarkSeen("baby1", 1)
	if !d.IsSeen("baby1", 1) {
		t.Error("baby1/1 should be seen")
	}
	// Same numeric ID for another baby is distinct.
	if d.IsSeen("baby2", 1) {
		t.Error("baby2/1 must not be seen after marking baby1/1")
	}
	d.MarkSeen("baby2", 1)
	if !d.IsSeen("baby2", 1) {
		t.Error("baby2/1 should be seen after marking")
	}
	if d.Size() != 2 {
		t.Errorf("Size = %d, want 2 for two baby-scoped IDs", d.Size())
	}
}

func TestDeduplicatorEviction(t *testing.T) {
	maxSize := 10
	d := NewDeduplicator(maxSize)

	// Add more than maxSize messages
	for i := 1; i <= 20; i++ {
		d.MarkSeen("baby1", i)
	}

	// Older messages should be evicted (1-10)
	// Newer messages should remain (11-20)
	for i := 1; i <= 10; i++ {
		if d.IsSeen("baby1", i) {
			t.Errorf("Old message %d should have been evicted", i)
		}
	}

	for i := 11; i <= 20; i++ {
		if !d.IsSeen("baby1", i) {
			t.Errorf("New message %d should still be seen", i)
		}
	}
}

func TestDeduplicatorEvictionKeepsNewest(t *testing.T) {
	maxSize := 5
	d := NewDeduplicator(maxSize)

	// Add messages in order
	for i := 1; i <= 10; i++ {
		d.MarkSeen("baby1", i)
	}

	// Should keep the newest 5 (6-10)
	count := 0
	for i := 1; i <= 10; i++ {
		if d.IsSeen("baby1", i) {
			count++
		}
	}

	if count > maxSize {
		t.Errorf("Should have at most %d messages, got %d", maxSize, count)
	}
}

func TestDeduplicatorConcurrentAccess(t *testing.T) {
	d := NewDeduplicator(1000)

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Spawn goroutines that mark and check messages
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numOperations; i++ {
				msgID := goroutineID*numOperations + i
				d.MarkSeen("baby1", msgID)
				d.IsSeen("baby1", msgID)
			}
		}(g)
	}

	wg.Wait()

	// No panic = success for concurrent access
}

func TestDeduplicatorSize(t *testing.T) {
	d := NewDeduplicator(100)

	if d.Size() != 0 {
		t.Errorf("Initial size should be 0, got %d", d.Size())
	}

	d.MarkSeen("baby1", 1)
	d.MarkSeen("baby1", 2)
	d.MarkSeen("baby1", 3)

	if d.Size() != 3 {
		t.Errorf("Size should be 3, got %d", d.Size())
	}

	// Marking same ID again shouldn't increase size
	d.MarkSeen("baby1", 1)
	if d.Size() != 3 {
		t.Errorf("Size should still be 3, got %d", d.Size())
	}
}

func TestDeduplicatorClear(t *testing.T) {
	d := NewDeduplicator(100)

	d.MarkSeen("baby1", 1)
	d.MarkSeen("baby1", 2)
	d.MarkSeen("baby1", 3)

	if d.Size() != 3 {
		t.Errorf("Size should be 3, got %d", d.Size())
	}

	d.Clear()

	if d.Size() != 0 {
		t.Errorf("Size should be 0 after clear, got %d", d.Size())
	}

	if d.IsSeen("baby1", 1) {
		t.Error("Message 1 should not be seen after clear")
	}
}

func TestDeduplicatorZeroMaxSize(t *testing.T) {
	// Edge case: maxSize of 0 should still work (no eviction)
	d := NewDeduplicator(0)

	d.MarkSeen("baby1", 1)
	d.MarkSeen("baby1", 2)

	// With maxSize 0, we treat it as unlimited
	if !d.IsSeen("baby1", 1) {
		t.Error("Message 1 should be seen")
	}
	if !d.IsSeen("baby1", 2) {
		t.Error("Message 2 should be seen")
	}
}
