package mqtt

import (
	"strings"
	"sync"
	"testing"

	MQTT "github.com/eclipse/paho.mqtt.golang"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

func TestEnsureClientSynchronous(t *testing.T) {
	c := NewConnection(Opts{TopicPrefix: "nanit", BrokerURL: "tcp://127.0.0.1:1883"})
	if got := c.GetClient(); got != nil {
		t.Fatalf("initial GetClient = %v, want nil", got)
	}
	first := c.EnsureClient()
	if first == nil {
		t.Fatal("EnsureClient returned nil")
	}
	if got := c.GetClient(); got != first {
		t.Error("GetClient after EnsureClient must return same client without async Run")
	}
	second := c.EnsureClient()
	if second != first {
		t.Error("EnsureClient must reuse existing client")
	}
}

func TestClientOptionsReconnectLifecycle(t *testing.T) {
	c := NewConnection(Opts{TopicPrefix: "nanit", BrokerURL: "tcp://127.0.0.1:1883"})
	c.mu.Lock()
	opts := c.buildClientOptionsLocked()
	c.mu.Unlock()
	if !opts.AutoReconnect {
		t.Error("AutoReconnect must be true")
	}
	if opts.OnConnect == nil {
		t.Error("OnConnect handler must be set")
	}
	if opts.OnConnectionLost == nil {
		t.Error("OnConnectionLost handler must be set")
	}
}

func fireInstalledConnect(t *testing.T, conn *Connection, fc *fakeClient) {
	t.Helper()
	conn.mu.Lock()
	opts := conn.buildClientOptionsLocked()
	conn.mu.Unlock()
	if opts.OnConnect == nil {
		t.Fatal("OnConnect not installed")
	}
	opts.OnConnect(fc)
}

func fireInstalledLoss(t *testing.T, conn *Connection) {
	t.Helper()
	conn.mu.Lock()
	opts := conn.buildClientOptionsLocked()
	conn.mu.Unlock()
	if opts.OnConnectionLost == nil {
		t.Fatal("OnConnectionLost not installed")
	}
	opts.OnConnectionLost(nil, nil)
}

func clearPublishes(fc *fakeClient) {
	fc.mu.Lock()
	fc.publishes = nil
	fc.mu.Unlock()
}

func hasTopicSuffix(fc *fakeClient, suffix string) bool {
	for _, p := range fc.publishedTopics() {
		if strings.HasSuffix(p.topic, suffix) {
			return true
		}
	}
	return false
}

func countTopic(fc *fakeClient, suffix, payload string) int {
	n := 0
	for _, p := range fc.publishedTopics() {
		if strings.HasSuffix(p.topic, suffix) && p.payload == payload {
			n++
		}
	}
	return n
}

func TestCallbackLifecycleRestoresRetainedStore(t *testing.T) {
	opts := Opts{TopicPrefix: "nanit", DiscoveryEnabled: true, DiscoveryPrefix: "homeassistant"}
	conn, fc, timers := newConnForTest(opts)
	conn.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "Baby One"}})

	// Initial connect through the installed Paho callback (single owner).
	fireInstalledConnect(t, conn, fc)
	found := func(suffix, payload string) bool {
		for _, p := range fc.publishedTopics() {
			if strings.HasSuffix(p.topic, suffix) && p.payload == payload {
				return true
			}
		}
		return false
	}
	if !found("nanit/status", "online") {
		t.Fatal("initial callback must publish online status")
	}
	if !found("/babies/baby1/motion_active", "false") {
		t.Fatal("initial callback must init motion_active false")
	}
	if !hasTopicSuffix(fc, "/temperature/config") {
		t.Fatal("initial callback must republish discovery configs")
	}
	fc.mu.Lock()
	subs := len(fc.subs)
	fc.mu.Unlock()
	if subs < 2 {
		t.Fatalf("initial callback must restore command subscriptions, got %d", subs)
	}

	// Simulate broker loss clearing retained state, then loss callback.
	clearPublishes(fc)
	fc.mu.Lock()
	fc.subs = map[string]MQTT.MessageHandler{}
	fc.mu.Unlock()

	// Create a live timer before loss so invalidation has something to clear.
	conn.publishEvent("baby1", "motion", 1700000000)
	var oldTimer *fakeTimer
	if len(*timers) > 0 {
		oldTimer = (*timers)[len(*timers)-1]
	}
	fireInstalledLoss(t, conn)

	conn.mu.Lock()
	timersAfterLoss := len(conn.timers)
	conn.mu.Unlock()
	if timersAfterLoss != 0 {
		t.Fatalf("loss must invalidate timers, got %d", timersAfterLoss)
	}

	// Reconnect through the installed callback must restore from empty store.
	clearPublishes(fc)
	fireInstalledConnect(t, conn, fc)
	if !found("nanit/status", "online") {
		t.Error("reconnect must publish online status from empty store")
	}
	if !found("/babies/baby1/motion_active", "false") {
		t.Error("reconnect must init motion_active false from empty store")
	}
	if !hasTopicSuffix(fc, "/temperature/config") {
		t.Error("reconnect must republish discovery from empty store")
	}
	fc.mu.Lock()
	subs = len(fc.subs)
	fc.mu.Unlock()
	if subs < 2 {
		t.Errorf("reconnect must restore command subscriptions, got %d", subs)
	}
	// Old generation cannot publish after reconnect restoration.
	falsesBeforeOld := countTopic(fc, "/babies/baby1/motion_active", "false")
	if oldTimer == nil {
		t.Fatal("expected old timer before reconnect")
	}
	oldTimer.fire()
	if got := countTopic(fc, "/babies/baby1/motion_active", "false"); got != falsesBeforeOld {
		t.Errorf("old generation published after reconnect, falses %d -> %d", falsesBeforeOld, got)
	}

	// New events re-arm with monotonic generations.
	conn.publishEvent("baby1", "motion", 1700000060)
	if !found("/babies/baby1/motion_active", "true") {
		t.Error("post-reconnect event must publish active true")
	}
}

func TestSupersessionPublicationBarrier(t *testing.T) {
	opts := Opts{TopicPrefix: "nanit", DiscoveryEnabled: false}
	conn, fc, timers := newConnForTest(opts)

	// Barrier: concurrent events for the same key start together.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(epoch int32) {
			defer wg.Done()
			<-start
			conn.publishEvent("baby1", "motion", epoch)
		}(int32(1700000000 + i))
	}
	close(start)
	wg.Wait()

	if len(*timers) != 10 {
		t.Fatalf("expected 10 timers, got %d", len(*timers))
	}
	// Fire all timers concurrently under a second barrier; only the latest
	// generation may publish false.
	fire := make(chan struct{})
	var fwg sync.WaitGroup
	for _, tm := range *timers {
		fwg.Add(1)
		go func(tm *fakeTimer) {
			defer fwg.Done()
			<-fire
			tm.fire()
		}(tm)
	}
	close(fire)
	fwg.Wait()
	if got := countTopic(fc, "/babies/baby1/motion_active", "false"); got != 1 {
		t.Errorf("concurrent superseded timers published %d falses, want exactly 1", got)
	}
}

func TestLossShutdownPublicationBarriers(t *testing.T) {
	opts := Opts{TopicPrefix: "nanit", DiscoveryEnabled: false}

	// Concurrent event vs teardown: either the event wins (one true, timer
	// cleared, no false after) or teardown wins (no true). After teardown,
	// firing timers must add no new false.
	conn, fc, timers := newConnForTest(opts)
	barrier := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-barrier
		conn.publishEvent("baby1", "sound", 1700000100)
	}()
	go func() {
		defer wg.Done()
		<-barrier
		conn.mu.Lock()
		conn.shutdown = true
		conn.mu.Unlock()
		conn.invalidateTimers()
	}()
	close(barrier)
	wg.Wait()
	afterRace := countTopic(fc, "/babies/baby1/sound_active", "false")
	for _, tm := range *timers {
		tm.fire()
	}
	if got := countTopic(fc, "/babies/baby1/sound_active", "false"); got != afterRace {
		t.Errorf("callback after teardown added publish (%d -> %d), want no new", afterRace, got)
	}
	trues := countTopic(fc, "/babies/baby1/sound_active", "true")
	if trues > 1 {
		t.Errorf("concurrent event/teardown published %d trues, want at most 1", trues)
	}

	// Stale shutdown callback must not reopen.
	conn2, fc2, _ := newConnForTest(opts)
	conn2.RegisterBabies([]baby.Baby{{UID: "baby1", Name: "B"}})
	fireInstalledConnect(t, conn2, fc2)
	clearPublishes(fc2)
	conn2.mu.Lock()
	conn2.shutdown = true
	conn2.mu.Unlock()
	conn2.invalidateTimers()
	fireInstalledConnect(t, conn2, fc2)
	for _, p := range fc2.publishedTopics() {
		if strings.HasSuffix(p.topic, "nanit/status") && p.payload == "online" {
			t.Error("stale shutdown connect must not republish online")
		}
	}
	conn2.mu.Lock()
	closed := conn2.closed
	conn2.mu.Unlock()
	if !closed {
		t.Error("stale shutdown connect must not reopen (closed must stay true)")
	}
	// Publish after shutdown must not publish.
	conn2.publishEvent("baby1", "motion", 1700000200)
	if got := len(fc2.publishedTopics()); got != 0 {
		t.Errorf("publish after shutdown published %d messages, want 0", got)
	}
	// Clearing shutdown (next runMqtt attempt) re-arms.
	conn2.mu.Lock()
	conn2.shutdown = false
	conn2.mu.Unlock()
	fireInstalledConnect(t, conn2, fc2)
	if !hasTopicSuffix(fc2, "nanit/status") {
		t.Error("connect after shutdown cleared must publish status")
	}

	// Monotonic generations across reconnect via callbacks.
	conn3, fc3, timers3 := newConnForTest(opts)
	conn3.publishEvent("baby1", "motion", 1700000300)
	old := (*timers3)[0]
	fireInstalledLoss(t, conn3)
	fireInstalledConnect(t, conn3, fc3)
	conn3.publishEvent("baby1", "motion", 1700000360)
	if len(*timers3) != 2 {
		t.Fatalf("expected 2 timers across reconnect, got %d", len(*timers3))
	}
	old.fire()
	if got := countTopic(fc3, "/babies/baby1/motion_active", "false"); got != 0 {
		t.Errorf("old generation published after reconnect, got %d falses", got)
	}
	(*timers3)[1].fire()
	if got := countTopic(fc3, "/babies/baby1/motion_active", "false"); got != 1 {
		t.Errorf("current generation should publish false once, got %d", got)
	}
}
