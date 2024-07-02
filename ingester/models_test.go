package ingester_test

import (
	"testing"

	"github.com/duneanalytics/blockchain-ingester/ingester"
	"github.com/go-errors/errors"
	"github.com/stretchr/testify/require"
)

// TestInfoErrors ensures that we never allow the error slices to grow indefinitely
func TestInfoErrors(t *testing.T) {
	info := ingester.NewInfo("test", "test")
	for j := 0; j < 2; j++ {
		for i := 0; i < 200; i++ {
			require.Len(t, info.Errors.RPCErrors, min(i, 100))
			require.Len(t, info.Errors.DuneErrors, min(i, 100))
			info.Errors.ObserveDuneError(ingester.ErrorInfo{})
			info.Errors.ObserveRPCError(ingester.ErrorInfo{})
			require.Equal(t, 100, cap(info.Errors.RPCErrors))
			require.Equal(t, 100, cap(info.Errors.DuneErrors))
		}
		info.ResetErrors()
		require.Len(t, info.Errors.RPCErrors, 0)
		require.Len(t, info.Errors.DuneErrors, 0)
		require.Equal(t, 100, cap(info.Errors.RPCErrors))
		require.Equal(t, 100, cap(info.Errors.DuneErrors))
	}
}

func TestProgressReportErrors(t *testing.T) {
	info := ingester.NewInfo("test", "test")
	info.Errors.ObserveDuneError(ingester.ErrorInfo{Error: errors.New("foo")})
	info.Errors.ObserveRPCError(ingester.ErrorInfo{Error: errors.New("bar")})
	errors := info.ProgressReportErrors()
	require.Len(t, errors, 2)
}

func TestInfoToProgressReport(t *testing.T) {
	info := ingester.NewInfo("test", "test")
	info.IngestedBlockNumber = 1
	info.LatestBlockNumber = 2
	info.Errors.ObserveDuneError(ingester.ErrorInfo{Error: errors.New("foo")})
	report := info.ToProgressReport()
	require.Equal(t, "test", report.BlockchainName)
	require.Equal(t, "test", report.EVMStack)
	require.Equal(t, int64(1), report.LastIngestedBlockNumber)
	require.Equal(t, int64(2), report.LatestBlockNumber)
	require.Len(t, report.Errors, 1)
	require.Equal(t, 1, report.DuneErrorCounts)
	require.Equal(t, 0, report.RPCErrorCounts)
}
