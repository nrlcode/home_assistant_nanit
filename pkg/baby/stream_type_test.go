package baby_test

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
)

// ============================================================
// TESTS FOR NEW STREAM TYPE
// These tests define the expected behavior for the new StreamType
// enum that will be used for local/remote stream failover.
// Following TDD, these tests are written BEFORE implementation.
// ============================================================

// TestStreamTypeValues verifies the StreamType enum values exist
func TestStreamTypeValues(t *testing.T) {
	// StreamType should have these values
	assert.Equal(t, baby.StreamType(0), baby.StreamType_None)
	assert.Equal(t, baby.StreamType(1), baby.StreamType_Local)
	assert.Equal(t, baby.StreamType(2), baby.StreamType_Remote)
}

// TestStateGetStreamType tests the getter for stream type
func TestStateGetStreamType(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*baby.State)
		expected baby.StreamType
	}{
		{
			name:     "nil returns None",
			setup:    func(s *baby.State) {},
			expected: baby.StreamType_None,
		},
		{
			name: "Local returns Local",
			setup: func(s *baby.State) {
				s.SetStreamType(baby.StreamType_Local)
			},
			expected: baby.StreamType_Local,
		},
		{
			name: "Remote returns Remote",
			setup: func(s *baby.State) {
				s.SetStreamType(baby.StreamType_Remote)
			},
			expected: baby.StreamType_Remote,
		},
		{
			name: "None returns None",
			setup: func(s *baby.State) {
				s.SetStreamType(baby.StreamType_None)
			},
			expected: baby.StreamType_None,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &baby.State{}
			tt.setup(s)
			assert.Equal(t, tt.expected, s.GetStreamType())
		})
	}
}

// TestStateSetStreamType tests the setter for stream type
func TestStateSetStreamType(t *testing.T) {
	s := baby.NewState()

	// Setter should return state for chaining
	result := s.SetStreamType(baby.StreamType_Local)
	assert.Same(t, s, result)

	// Value should be set
	assert.Equal(t, baby.StreamType_Local, s.GetStreamType())

	// Can change value
	s.SetStreamType(baby.StreamType_Remote)
	assert.Equal(t, baby.StreamType_Remote, s.GetStreamType())
}

// TestStateStreamTypeMerge tests that StreamType merges correctly
func TestStateStreamTypeMerge(t *testing.T) {
	original := baby.NewState().
		SetStreamType(baby.StreamType_Local).
		SetTemperatureMilli(25000)

	update := baby.NewState().
		SetStreamType(baby.StreamType_Remote)

	merged := original.Merge(update)

	// StreamType should be updated
	assert.Equal(t, baby.StreamType_Remote, merged.GetStreamType())
	// Temperature should be preserved
	assert.Equal(t, 25.0, merged.GetTemperature())
}

// TestStateStreamTypeAsMap tests that StreamType is included in AsMap when internal=true
func TestStateStreamTypeAsMap(t *testing.T) {
	s := baby.NewState().
		SetStreamType(baby.StreamType_Local)

	// Should be included when includeInternal=true
	m := s.AsMap(true)
	assert.Contains(t, m, "stream_type")
	assert.Equal(t, int64(baby.StreamType_Local), m["stream_type"])

	// Should NOT be included when includeInternal=false (it's internal)
	m = s.AsMap(false)
	assert.NotContains(t, m, "stream_type")
}

// TestStateStreamTypeWithOtherFields tests StreamType alongside other state fields
func TestStateStreamTypeWithOtherFields(t *testing.T) {
	s := baby.NewState().
		SetStreamType(baby.StreamType_Remote).
		SetStreamState(baby.StreamState_Alive).
		SetStreamRequestState(baby.StreamRequestState_Requested).
		SetWebsocketAlive(true).
		SetTemperatureMilli(25000)

	assert.Equal(t, baby.StreamType_Remote, s.GetStreamType())
	assert.Equal(t, baby.StreamState_Alive, s.GetStreamState())
	assert.Equal(t, baby.StreamRequestState_Requested, s.GetStreamRequestState())
	assert.True(t, s.GetIsWebsocketAlive())
	assert.Equal(t, 25.0, s.GetTemperature())
}

// TestStreamTypeString tests the string representation of StreamType
func TestStreamTypeString(t *testing.T) {
	tests := []struct {
		streamType baby.StreamType
		expected   string
	}{
		{baby.StreamType_None, "None"},
		{baby.StreamType_Local, "Local"},
		{baby.StreamType_Remote, "Remote"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.streamType.String())
		})
	}
}
