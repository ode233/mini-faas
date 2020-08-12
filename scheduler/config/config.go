package config

import "time"

const (
	MaxNodeNum = 10

	ReleaseResourcesTimeout = 10 * time.Second

	MaxBaseTimeRatio = 1.5

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
