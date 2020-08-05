package config

import "time"

const (
	MaxNodeNum = 10

	// 隔多少次请求打印一次节点日志
	LogPrintInterval = 20
	NeedLog          = false

	Timout         = 30 * time.Second
	ChannelTimeout = 10 * time.Millisecond
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
