package mqtt

import (
	"testing"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

// Media-measured retained state: discovery always publishes the retained
// boolean, false until media observed, true when already alive.
func TestDiscoveryRetainedAliveReflectsMeasuredState(t *testing.T) {
	opts := Opts{TopicPrefix: "nanit", DiscoveryEnabled: true, DiscoveryPrefix: "homeassistant"}

	c, fc, _ := newConnForTest(opts)
	c.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "B"}})
	c.StateManager = baby.NewStateManager()
	c.publishDiscovery("baby1", "B")
	got := fc.findTopic("/babies/baby1/is_stream_alive")
	if got == nil || got.payload != "false" || !got.retained {
		t.Fatalf("initial retained alive = %+v, want false retained", got)
	}

	c2, fc2, _ := newConnForTest(opts)
	c2.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "B"}})
	c2.StateManager = baby.NewStateManager()
	c2.StateManager.Update("baby1", *baby.NewState().SetStreamState(baby.StreamState_Alive))
	c2.publishDiscovery("baby1", "B")
	got2 := fc2.findTopic("/babies/baby1/is_stream_alive")
	if got2 == nil || got2.payload != "true" || !got2.retained {
		t.Fatalf("alive retained = %+v, want true retained", got2)
	}
}
