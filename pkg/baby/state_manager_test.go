package baby_test

import (
	"sync"
	"testing"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

// ============================================================
// REGRESSION TESTS FOR STATE MANAGER
// These tests ensure existing functionality works before we
// make changes for stream failover implementation.
// ============================================================

func TestNewStateManager(t *testing.T) {
	manager := baby.NewStateManager()
	assert.NotNil(t, manager)
}

func TestStateManagerUpdateNewBaby(t *testing.T) {
	manager := baby.NewStateManager()

	state := baby.State{}
	state.SetTemperatureMilli(25000)

	manager.Update("baby123", state)

	result := manager.GetBabyState("baby123")
	assert.Equal(t, 25.0, result.GetTemperature())
}

func TestStateManagerUpdateExistingBaby(t *testing.T) {
	manager := baby.NewStateManager()

	// First update
	state1 := baby.State{}
	state1.SetTemperatureMilli(25000)
	manager.Update("baby123", state1)

	// Second update with different field
	state2 := baby.State{}
	state2.SetHumidityMilli(60000)
	manager.Update("baby123", state2)

	// Both fields should be present (merge behavior)
	result := manager.GetBabyState("baby123")
	assert.Equal(t, 25.0, result.GetTemperature())
	assert.Equal(t, 60.0, result.GetHumidity())
}

func TestStateManagerUpdateOverwritesExistingField(t *testing.T) {
	manager := baby.NewStateManager()

	// First update
	state1 := baby.State{}
	state1.SetTemperatureMilli(25000)
	manager.Update("baby123", state1)

	// Second update overwrites temperature
	state2 := baby.State{}
	state2.SetTemperatureMilli(30000)
	manager.Update("baby123", state2)

	result := manager.GetBabyState("baby123")
	assert.Equal(t, 30.0, result.GetTemperature())
}

func TestStateManagerGetBabyStateUnknownBaby(t *testing.T) {
	manager := baby.NewStateManager()

	result := manager.GetBabyState("unknown")
	// Should return nil for unknown baby
	assert.Nil(t, result)
}

func TestStateManagerMultipleBabies(t *testing.T) {
	manager := baby.NewStateManager()

	state1 := baby.State{}
	state1.SetTemperatureMilli(25000)
	manager.Update("baby1", state1)

	state2 := baby.State{}
	state2.SetTemperatureMilli(26000)
	manager.Update("baby2", state2)

	result1 := manager.GetBabyState("baby1")
	result2 := manager.GetBabyState("baby2")

	assert.Equal(t, 25.0, result1.GetTemperature())
	assert.Equal(t, 26.0, result2.GetTemperature())
}

func TestStateManagerSubscribe(t *testing.T) {
	manager := baby.NewStateManager()

	type notification struct {
		uid   string
		state baby.State
	}
	notifyChan := make(chan notification, 1)

	unsubscribe := manager.Subscribe(func(babyUID string, state baby.State) {
		select {
		case notifyChan <- notification{uid: babyUID, state: state}:
		default:
		}
	})
	defer unsubscribe()

	state := baby.State{}
	state.SetTemperatureMilli(25000)
	manager.Update("baby123", state)

	select {
	case n := <-notifyChan:
		assert.Equal(t, "baby123", n.uid)
		assert.Equal(t, 25.0, n.state.GetTemperature())
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for subscriber notification")
	}
}

func TestStateManagerSubscribeReceivesExistingStates(t *testing.T) {
	manager := baby.NewStateManager()

	// Add state before subscribing
	state := baby.State{}
	state.SetTemperatureMilli(25000)
	manager.Update("baby123", state)

	receivedChan := make(chan string, 1)

	unsubscribe := manager.Subscribe(func(babyUID string, state baby.State) {
		select {
		case receivedChan <- babyUID:
		default:
		}
	})
	defer unsubscribe()

	select {
	case receivedUID := <-receivedChan:
		assert.Equal(t, "baby123", receivedUID)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for existing state notification")
	}
}

func TestStateManagerUnsubscribe(t *testing.T) {
	manager := baby.NewStateManager()

	callChan := make(chan struct{}, 10)

	unsubscribe := manager.Subscribe(func(babyUID string, state baby.State) {
		select {
		case callChan <- struct{}{}:
		default:
		}
	})

	// First update - should trigger callback
	state1 := baby.State{}
	state1.SetTemperatureMilli(25000)
	manager.Update("baby123", state1)

	// Wait for first notification
	select {
	case <-callChan:
		// Good, received first notification
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first notification")
	}

	// Unsubscribe
	unsubscribe()

	// Second update - should NOT trigger callback
	state2 := baby.State{}
	state2.SetTemperatureMilli(26000)
	manager.Update("baby123", state2)

	// Wait a bit and verify no more notifications
	select {
	case <-callChan:
		t.Fatal("Received notification after unsubscribe")
	case <-time.After(100 * time.Millisecond):
		// Good, no notification after unsubscribe
	}
}

func TestStateManagerMultipleSubscribers(t *testing.T) {
	manager := baby.NewStateManager()

	sub1Chan := make(chan struct{}, 1)
	sub2Chan := make(chan struct{}, 1)

	unsub1 := manager.Subscribe(func(babyUID string, state baby.State) {
		select {
		case sub1Chan <- struct{}{}:
		default:
		}
	})
	defer unsub1()

	unsub2 := manager.Subscribe(func(babyUID string, state baby.State) {
		select {
		case sub2Chan <- struct{}{}:
		default:
		}
	})
	defer unsub2()

	state := baby.State{}
	state.SetTemperatureMilli(25000)
	manager.Update("baby123", state)

	// Wait for both subscribers
	timeout := time.After(1 * time.Second)
	sub1Called := false
	sub2Called := false

	for !sub1Called || !sub2Called {
		select {
		case <-sub1Chan:
			sub1Called = true
		case <-sub2Chan:
			sub2Called = true
		case <-timeout:
			t.Fatalf("Timeout: sub1Called=%v, sub2Called=%v", sub1Called, sub2Called)
		}
	}

	assert.True(t, sub1Called)
	assert.True(t, sub2Called)
}

func TestStateManagerNotifyMotionSubscribers(t *testing.T) {
	manager := baby.NewStateManager()

	stateChan := make(chan baby.State, 1)

	unsubscribe := manager.Subscribe(func(babyUID string, state baby.State) {
		if state.MotionTimestamp != nil {
			select {
			case stateChan <- state:
			default:
			}
		}
	})
	defer unsubscribe()

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	manager.NotifyMotionSubscribers("baby123", testTime)

	select {
	case receivedState := <-stateChan:
		assert.NotNil(t, receivedState.MotionTimestamp)
		assert.Equal(t, int32(testTime.Unix()), *receivedState.MotionTimestamp)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for motion notification")
	}
}

func TestStateManagerNotifySoundSubscribers(t *testing.T) {
	manager := baby.NewStateManager()

	stateChan := make(chan baby.State, 1)

	unsubscribe := manager.Subscribe(func(babyUID string, state baby.State) {
		if state.SoundTimestamp != nil {
			select {
			case stateChan <- state:
			default:
			}
		}
	})
	defer unsubscribe()

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	manager.NotifySoundSubscribers("baby123", testTime)

	select {
	case receivedState := <-stateChan:
		assert.NotNil(t, receivedState.SoundTimestamp)
		assert.Equal(t, int32(testTime.Unix()), *receivedState.SoundTimestamp)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for sound notification")
	}
}

// TestStateManagerStreamStateUpdate tests updating stream state
// Important for failover implementation
func TestStateManagerStreamStateUpdate(t *testing.T) {
	manager := baby.NewStateManager()

	state := baby.State{}
	state.SetStreamState(baby.StreamState_Alive)
	state.SetStreamRequestState(baby.StreamRequestState_Requested)
	state.SetWebsocketAlive(true)

	manager.Update("baby123", state)

	result := manager.GetBabyState("baby123")
	assert.Equal(t, baby.StreamState_Alive, result.GetStreamState())
	assert.Equal(t, baby.StreamRequestState_Requested, result.GetStreamRequestState())
	assert.True(t, result.GetIsWebsocketAlive())
}

// TestStateManagerConcurrentUpdates tests thread safety
func TestStateManagerConcurrentUpdates(t *testing.T) {
	manager := baby.NewStateManager()

	var wg sync.WaitGroup
	numGoroutines := 10
	numUpdates := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numUpdates; j++ {
				state := baby.State{}
				state.SetTemperatureMilli(int32(goroutineID*1000 + j))
				manager.Update("baby123", state)
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and should have final state
	result := manager.GetBabyState("baby123")
	assert.NotNil(t, result)
}

// TestStateManagerConcurrentSubscribeUnsubscribe tests thread safety of subscriptions
func TestStateManagerConcurrentSubscribeUnsubscribe(t *testing.T) {
	manager := baby.NewStateManager()

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unsub := manager.Subscribe(func(babyUID string, state baby.State) {
				// noop
			})
			time.Sleep(10 * time.Millisecond)
			unsub()
		}()
	}

	// Also do updates concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			state := baby.State{}
			state.SetTemperatureMilli(int32(id * 1000))
			manager.Update("baby123", state)
		}(i)
	}

	wg.Wait()
	// Should not panic or deadlock
}
