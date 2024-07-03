package ingester

import (
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
)

type Info struct {
	BlockchainName      string
	Stack               string
	LatestBlockNumber   int64
	IngestedBlockNumber int64
	ConsumedBlockNumber int64
	Errors              ErrorState
	Since               time.Time
}

func NewInfo(blockchain string, stack string) Info {
	return Info{
		BlockchainName: blockchain,
		Stack:          stack,
		Errors: ErrorState{
			RPCErrors:      make([]ErrorInfo, 0, 100),
			DuneErrors:     make([]ErrorInfo, 0, 100),
			RPCErrorCount:  0,
			DuneErrorCount: 0,
		},
		Since: time.Now(),
	}
}

func (info *Info) ToProgressReport() models.BlockchainIndexProgress {
	return models.BlockchainIndexProgress{
		BlockchainName:          info.BlockchainName,
		EVMStack:                info.Stack,
		LastIngestedBlockNumber: info.IngestedBlockNumber,
		LatestBlockNumber:       info.LatestBlockNumber,
		Errors:                  info.ProgressReportErrors(),
		DuneErrorCounts:         info.Errors.DuneErrorCount,
		RPCErrorCounts:          info.Errors.RPCErrorCount,
		Since:                   info.Since,
	}
}

func (info *Info) ResetErrors() {
	info.Since = time.Now()
	info.Errors.Reset()
}

type ErrorState struct {
	RPCErrors      []ErrorInfo
	DuneErrors     []ErrorInfo
	RPCErrorCount  int
	DuneErrorCount int
}

// ProgressReportErrors returns a combined list of errors from RPC requests and Dune requests
func (info Info) ProgressReportErrors() []models.BlockchainIndexError {
	errors := make([]models.BlockchainIndexError, 0, len(info.Errors.RPCErrors)+len(info.Errors.DuneErrors))
	for _, e := range info.Errors.RPCErrors {
		errors = append(errors, models.BlockchainIndexError{
			Timestamp:    e.Timestamp,
			BlockNumbers: e.BlockNumbers,
			Error:        e.Error.Error(),
			Source:       "rpc",
		})
	}
	for _, e := range info.Errors.DuneErrors {
		errors = append(errors, models.BlockchainIndexError{
			Timestamp:    e.Timestamp,
			BlockNumbers: e.BlockNumbers,
			Error:        e.Error.Error(),
			Source:       "dune",
		})
	}
	return errors
}

func (es *ErrorState) Reset() {
	es.RPCErrors = es.RPCErrors[:0]
	es.DuneErrors = es.DuneErrors[:0]
	es.RPCErrorCount = 0
	es.DuneErrorCount = 0
}

func (es *ErrorState) ObserveRPCError(err ErrorInfo) {
	es.RPCErrorCount++
	err.Timestamp = time.Now()

	// If we have filled the slice, remove the oldest error
	if len(es.RPCErrors) == cap(es.RPCErrors) {
		tmp := make([]ErrorInfo, len(es.RPCErrors)-1, cap(es.RPCErrors))
		copy(tmp, es.RPCErrors[1:])
		es.RPCErrors = tmp
	}
	es.RPCErrors = append(es.RPCErrors, err)
}

func (es *ErrorState) ObserveDuneError(err ErrorInfo) {
	es.DuneErrorCount++
	err.Timestamp = time.Now()

	// If we have filled the slice, remove the oldest error
	if len(es.DuneErrors) == cap(es.DuneErrors) {
		tmp := make([]ErrorInfo, len(es.DuneErrors)-1, cap(es.DuneErrors))
		copy(tmp, es.DuneErrors[1:])
		es.DuneErrors = tmp
	}
	es.DuneErrors = append(es.DuneErrors, err)
}

type ErrorInfo struct {
	Timestamp    time.Time
	BlockNumbers string
	Error        error
}
