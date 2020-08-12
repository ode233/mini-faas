package config

import "time"

const (
	MaxNodeNum = 10

	ReleaseResourcesTimeout = 20 * time.Second

	MaxBaseTimeRatio = 2

	LastWaitChannelTimeout = 40 * time.Second

	Timout = 30 * time.Second
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
