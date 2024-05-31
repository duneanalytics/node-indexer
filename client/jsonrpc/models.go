package jsonrpc

import "time"

type Config struct {
	URL          string
	PollInterval time.Duration
}
