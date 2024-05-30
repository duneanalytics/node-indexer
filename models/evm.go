package models

type EVMStack string

const (
	OpStack       EVMStack = "opstack"
	ArbitrumNitro EVMStack = "arbitrumnitro"
)

func (e EVMStack) String() string {
	return string(e)
}
