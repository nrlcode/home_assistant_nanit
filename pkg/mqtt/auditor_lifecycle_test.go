package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/indiefan/home_assistant_nanit/pkg/utils"
	"strings"
	"sync"
	"testing"
	"time"
)

type auditBarrierClient struct {
	*fakeClient
	once                       sync.Once
	entered, release, finished chan struct{}
	onConnect                  MQTT.OnConnectHandler
}

func (f *auditBarrierClient) Connect() MQTT.Token {
	go func() { f.onConnect(f); close(f.finished) }()
	return newFakeToken(nil)
}
func (f *auditBarrierClient) Publish(topic string, qos byte, retain bool, payload interface{}) MQTT.Token {
	if strings.HasSuffix(topic, "/config") {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.fakeClient.Publish(topic, qos, retain, payload)
}

func TestAuditorConnectCallbackJoinedBeforeTeardown(t *testing.T) {
	c := NewConnection(Opts{TopicPrefix: "nanit", DiscoveryEnabled: true})
	c.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "B"}})
	f := &auditBarrierClient{fakeClient: newFakeClient(), entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	c.mu.Lock()
	c.client = f
	f.onConnect = c.buildClientOptionsLocked().OnConnect
	c.mu.Unlock()
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) { c.Run(baby.NewStateManager(), ctx) })
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not enter discovery")
	}
	// Cancellation must wait for the admitted callback without deadlocking the
	// barrier that represents its in-flight publication.
	cancelled := make(chan struct{})
	go func() {
		runner.Cancel()
		close(cancelled)
	}()
	close(f.release)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("Run teardown did not join callback")
	}
	afterReturn := len(f.publishedTopics())
	select {
	case <-f.finished:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	if after := len(f.publishedTopics()); after != afterReturn {
		t.Fatalf("successful-connect callback published %d messages after Run teardown returned", after-afterReturn)
	}
}

func TestAuditorReconnectRestoresMeasuredAlive(t *testing.T) {
	c, f, _ := newConnForTest(Opts{TopicPrefix: "nanit", DiscoveryEnabled: true})
	c.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "B"}})
	c.StateManager = baby.NewStateManager()
	c.StateManager.Update("baby1", *baby.NewState().SetStreamState(baby.StreamState_Alive))
	fireInstalledConnect(t, c, f)
	clearPublishes(f)
	fireInstalledLoss(t, c)
	fireInstalledConnect(t, c, f)
	if got := f.findTopic("/babies/baby1/is_stream_alive"); got == nil || got.payload != "true" || !got.retained {
		t.Fatal("reconnect with measured alive state did not restore retained true after broker data loss")
	}
}
