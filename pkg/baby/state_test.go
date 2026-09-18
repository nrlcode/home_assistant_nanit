package baby_test

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

func TestStateAsMap(t *testing.T) {
	s := baby.State{}
	s.SetTemperatureMilli(1000)
	s.SetIsNight(true)
	s.SetStreamRequestState(baby.StreamRequestState_Requested)

	m := s.AsMap(false)

	assert.Equal(t, 1.0, m["temperature"], "The two words should be the same.")
	assert.Equal(t, true, m["is_night"], "The two words should be the same.")
	assert.NotContains(t, m, "is_stream_requested", "Should not contain internal fields")
}

func TestStateMergeSame(t *testing.T) {
	s1 := &baby.State{}
	s1.SetTemperatureMilli(10)

	s2 := &baby.State{}
	s2.SetTemperatureMilli(10)

	s3 := s1.Merge(s2)
	assert.Same(t, s1, s3)
}

func TestStateMergeDifferent(t *testing.T) {
	s1 := &baby.State{}
	s1.SetTemperatureMilli(10_000)
	s1.SetStreamState(baby.StreamState_Alive)

	s2 := &baby.State{}
	s2.SetTemperatureMilli(11_000)
	s2.SetHumidityMilli(20_000)
	s2.SetStreamState(baby.StreamState_Alive)

	s3 := s1.Merge(s2)
	assert.NotSame(t, s1, s3)
	assert.NotSame(t, s2, s3)
	assert.Equal(t, 10.0, s1.GetTemperature())

	assert.Equal(t, 11.0, s3.GetTemperature())
	assert.Equal(t, 20.0, s3.GetHumidity())
	assert.Equal(t, baby.StreamState_Alive, s3.GetStreamState())
}

// ============================================================
// REGRESSION TESTS FOR STREAM FAILOVER
// These tests ensure existing functionality works before we
// make changes for stream failover implementation.
// ============================================================

// TestGetIsWebsocketAlive tests the websocket alive getter
// BUG FIX: Original code checked StreamState instead of IsWebsocketAlive
func TestGetIsWebsocketAlive(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*baby.State)
		expected bool
	}{
		{
			name:     "nil state returns false",
			setup:    func(s *baby.State) {},
			expected: false,
		},
		{
			name: "websocket alive returns true",
			setup: func(s *baby.State) {
				s.SetWebsocketAlive(true)
			},
			expected: true,
		},
		{
			name: "websocket not alive returns false",
			setup: func(s *baby.State) {
				s.SetWebsocketAlive(false)
			},
			expected: false,
		},
		{
			name: "websocket alive without stream state still returns true",
			setup: func(s *baby.State) {
				// This test exposes the bug: if only IsWebsocketAlive is set
				// but StreamState is nil, it should still return true
				s.SetWebsocketAlive(true)
				// Note: NOT setting StreamState
			},
			expected: true,
		},
		{
			name: "websocket alive with stream state returns true",
			setup: func(s *baby.State) {
				s.SetWebsocketAlive(true)
				s.SetStreamState(baby.StreamState_Alive)
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &baby.State{}
			tt.setup(s)
			assert.Equal(t, tt.expected, s.GetIsWebsocketAlive())
		})
	}
}

// TestGetStreamState tests stream state getter with default values
func TestGetStreamState(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*baby.State)
		expected baby.StreamState
	}{
		{
			name:     "nil returns Unknown",
			setup:    func(s *baby.State) {},
			expected: baby.StreamState_Unknown,
		},
		{
			name: "Alive returns Alive",
			setup: func(s *baby.State) {
				s.SetStreamState(baby.StreamState_Alive)
			},
			expected: baby.StreamState_Alive,
		},
		{
			name: "Unhealthy returns Unhealthy",
			setup: func(s *baby.State) {
				s.SetStreamState(baby.StreamState_Unhealthy)
			},
			expected: baby.StreamState_Unhealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &baby.State{}
			tt.setup(s)
			assert.Equal(t, tt.expected, s.GetStreamState())
		})
	}
}

// TestGetStreamRequestState tests stream request state getter
func TestGetStreamRequestState(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*baby.State)
		expected baby.StreamRequestState
	}{
		{
			name:     "nil returns NotRequested",
			setup:    func(s *baby.State) {},
			expected: baby.StreamRequestState_NotRequested,
		},
		{
			name: "Requested returns Requested",
			setup: func(s *baby.State) {
				s.SetStreamRequestState(baby.StreamRequestState_Requested)
			},
			expected: baby.StreamRequestState_Requested,
		},
		{
			name: "RequestFailed returns RequestFailed",
			setup: func(s *baby.State) {
				s.SetStreamRequestState(baby.StreamRequestState_RequestFailed)
			},
			expected: baby.StreamRequestState_RequestFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &baby.State{}
			tt.setup(s)
			assert.Equal(t, tt.expected, s.GetStreamRequestState())
		})
	}
}

// TestStateSetterChaining verifies setters return the state for chaining
func TestStateSetterChaining(t *testing.T) {
	s := baby.NewState().
		SetTemperatureMilli(25000).
		SetHumidityMilli(50000).
		SetStreamState(baby.StreamState_Alive).
		SetStreamRequestState(baby.StreamRequestState_Requested).
		SetWebsocketAlive(true).
		SetIsNight(false).
		SetNightLight(true).
		SetStandby(false)

	assert.Equal(t, 25.0, s.GetTemperature())
	assert.Equal(t, 50.0, s.GetHumidity())
	assert.Equal(t, baby.StreamState_Alive, s.GetStreamState())
	assert.Equal(t, baby.StreamRequestState_Requested, s.GetStreamRequestState())
	assert.True(t, s.GetIsWebsocketAlive())
	assert.True(t, s.GetNightLight())
	assert.False(t, s.GetStandby())
}

// TestStateMergePreservesUnchangedFields ensures merge keeps fields not in update
func TestStateMergePreservesUnchangedFields(t *testing.T) {
	original := baby.NewState().
		SetTemperatureMilli(25000).
		SetStreamState(baby.StreamState_Alive).
		SetWebsocketAlive(true)

	update := baby.NewState().
		SetHumidityMilli(60000)

	merged := original.Merge(update)

	// Original fields preserved
	assert.Equal(t, 25.0, merged.GetTemperature())
	assert.Equal(t, baby.StreamState_Alive, merged.GetStreamState())
	assert.True(t, merged.GetIsWebsocketAlive())

	// New field added
	assert.Equal(t, 60.0, merged.GetHumidity())
}
