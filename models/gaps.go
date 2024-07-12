package models

type BlockchainGaps struct {
	Gaps []BlockGap
}

type BlockGap struct {
	FirstMissing int64
	LastMissing  int64
}
