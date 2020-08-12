package config

import "time"

const (
	MaxNodeNum = 20

	ReleaseResourcesTimeout = 60 * time.Second

	MaxBaseTimeRatio = 1.2

	FirstWaitChannelTimeout = 1 * time.Millisecond
	LastWaitChannelTimeout  = 40 * time.Second

	Timout = 30 * time.Second
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
