package config

import "time"

const (
	MaxNodeNum     = 10
	ReserveNodeNum = 5

	MemoryPerCpu = 1 * 1024 * 1024 * 1024 / 0.67

	ReserveContainerNumRadio   = 2
	ReserveContainerCacheRadio = 0.2

	// 隔多少次请求打印一次节点日志
	LogPrintInterval = 20
	NeedLog          = false

	Timout             = 30 * time.Second
	ChannelTimeout     = 10 * time.Millisecond
	WaitChannelTimeout = 10 * time.Second
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
