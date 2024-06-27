package models

type RPCBlock struct {
	BlockNumber int64
	// agnostic blob of data that is the block
	Payload []byte
	// optional field, if we fail to collect the block data
	Error error
}

func (b RPCBlock) Errored() bool {
	return b.Error != nil
}
