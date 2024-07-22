package models

type EVMStack string

const (
	OpStack       EVMStack = "opstack"
	ArbitrumNitro EVMStack = "arbitrum-nitro"
)

func (e EVMStack) String() string {
	return string(e)
}
