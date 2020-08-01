package config

const (
	MaxNodeNum     = 10
	ReserveNodeNum = 5

	FunctionNum = 15

	MaxContainerNum     = (4 * 1024 / 512) * MaxNodeNum / FunctionNum
	ReserveContainerNum = (4 * 1024 / 512) * ReserveNodeNum / FunctionNum

	// 隔多少次请求打印一次节点日志
	LogPrintInterval = 20
	NeedLog          = false
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
