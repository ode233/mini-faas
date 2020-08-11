package config

import "time"

const (
	MaxNodeNum = 20

	ReleaseResourcesTimeout    = 1 * time.Second
	ReleaseResourcesTimeoutNum = 10

	MaxBaseTimeRatio = 1.5

	BaseTimeChangeNum = 5

	FirstWaitChannelTimeout = 10 * time.Millisecond
	LastWaitChannelTimeout  = 40 * time.Second

	Timout = 30 * time.Second
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
