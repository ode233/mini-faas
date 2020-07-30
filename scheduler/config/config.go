package config

const (
	DefaultPort = 10450
	MaxNodeNum  = 20
	// 隔多少次请求打印一次日志
	LogPrintInterval = 2
	NeedLog          = true
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
