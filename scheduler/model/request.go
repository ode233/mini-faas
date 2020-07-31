package model

type RequestInfo struct {
	ID               string
	FunctionName     string
	RequestTimeoutMs int64
	AccountID        string
}

type ResponseInfo struct {
	RequestID             string
	ContainerId           string
	MaxMemoryUsageInBytes int64
	DurationInNanos       int64
}
