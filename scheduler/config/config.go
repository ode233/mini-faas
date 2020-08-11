package config

import "time"

const (
	MaxNodeNum = 10

	ReleaseResourcesTimeout = 1 * time.Second

	MaxBaseTimeRatio = 1.5

	BaseTimeChangeNum = 5

	Timout             = 30 * time.Second
	ChannelTimeout     = 10 * time.Millisecond
	WaitChannelTimeout = 40 * time.Second
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
