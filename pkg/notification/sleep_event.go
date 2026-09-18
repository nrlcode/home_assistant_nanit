package notification

import "github.com/indiefan/home_assistant_nanit/pkg/client"

const (
	SleepEventKeyFellAsleep    = client.SleepEventKeyFellAsleep
	SleepEventKeyWokeUp        = client.SleepEventKeyWokeUp
	SleepEventKeyPutInBed      = client.SleepEventKeyPutInBed
	SleepEventKeyPutToSleep    = client.SleepEventKeyPutToSleep
	SleepEventKeyRemoved       = client.SleepEventKeyRemoved
	SleepEventKeyRemovedAsleep = client.SleepEventKeyRemovedAsleep
	SleepEventKeyVisit         = client.SleepEventKeyVisit
)

type SleepEvent = client.SleepEvent
