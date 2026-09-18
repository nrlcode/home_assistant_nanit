package baby

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// StateManager - state manager context
type StateManager struct {
	babiesByUID      map[string]State
	subscribers      map[*chan bool]func(babyUID string, state State)
	stateMutex       sync.RWMutex
	subscribersMutex sync.RWMutex
}

// NewStateManager - state manager constructor
func NewStateManager() *StateManager {
	return &StateManager{
		babiesByUID: make(map[string]State),
		subscribers: make(map[*chan bool]func(babyUID string, state State)),
	}
}

// Update - updates baby info in thread safe manner
func (manager *StateManager) Update(babyUID string, stateUpdate State) {
	var updatedState *State

	manager.stateMutex.Lock()
	defer manager.stateMutex.Unlock()

	if babyState, ok := manager.babiesByUID[babyUID]; ok {
		updatedState = babyState.Merge(&stateUpdate)
		if updatedState == &babyState {
			return
		}
	} else {
		updatedState = NewState().Merge(&stateUpdate)
	}

	manager.babiesByUID[babyUID] = *updatedState
	stateUpdate.EnhanceLogEvent(log.Debug().Str("baby_uid", babyUID)).Msg("Baby state updated")

	go manager.notifySubscribers(babyUID, stateUpdate)
}

// Subscribe - registers function to be called on every update
// Returns unsubscribe function
func (manager *StateManager) Subscribe(callback func(babyUID string, state State)) func() {
	unsubscribeC := make(chan bool, 1)

	manager.subscribersMutex.Lock()
	manager.subscribers[&unsubscribeC] = callback
	manager.subscribersMutex.Unlock()

	manager.stateMutex.RLock()
	for babyUID, babyState := range manager.babiesByUID {
		// Capture loop variables to avoid race condition
		uid := babyUID
		state := babyState
		go callback(uid, state)
	}
	manager.stateMutex.RUnlock()

	return func() {
		manager.subscribersMutex.Lock()
		delete(manager.subscribers, &unsubscribeC)
		manager.subscribersMutex.Unlock()
	}
}

// GetBabyState - returns current state of a baby
// Returns nil if baby not found
func (manager *StateManager) GetBabyState(babyUID string) *State {
	manager.stateMutex.RLock()
	defer manager.stateMutex.RUnlock()

	babyState, ok := manager.babiesByUID[babyUID]
	if !ok {
		return nil
	}
	// Return pointer to a copy to prevent mutation of internal state
	stateCopy := babyState
	return &stateCopy
}

func (manager *StateManager) NotifyMotionSubscribers(babyUID string, time time.Time) {
	timestamp := new(int32)
	*timestamp = int32(time.Unix())
	var state = State{MotionTimestamp: timestamp}

	manager.notifySubscribers(babyUID, state)
}

func (manager *StateManager) NotifySoundSubscribers(babyUID string, time time.Time) {
	timestamp := new(int32)
	*timestamp = int32(time.Unix())
	var state = State{SoundTimestamp: timestamp}

	manager.notifySubscribers(babyUID, state)
}

func (manager *StateManager) notifySubscribers(babyUID string, state State) {
	manager.subscribersMutex.RLock()
	defer manager.subscribersMutex.RUnlock()

	for _, callback := range manager.subscribers {
		// Capture callback to avoid loop variable race condition
		cb := callback
		go cb(babyUID, state)
	}
}
