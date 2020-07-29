package config

const (
	DefaultPort = 10450
	MaxNodeNum  = 20
)

var Global = &Config{}

type Config struct {
	Region    string
	StackName string
	HostName  string
}
