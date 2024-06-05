package ingester_test

import "testing"

func TestBlockConsumptionLoop(t *testing.T) {
	testcases := []string{
		"we're very behind, trying to catch up",
		"we're up to date, following the head",
		"we're erroring systematically, the RPC node is broken, all API calls are failing",
		"we're erroring only on GetBlockByNumber, a specific jsonRPC on the RPC node is broken",
	}

	for _, testcase := range testcases {
		t.Run(testcase, func(t *testing.T) {
			t.Skip("not implemented")
		})
	}
}

func TestBlockSendingLoop(t *testing.T) {
	testcases := []string{
		"we're up to date, following the head",
		"we're failing intermittently, the Dune API is broken",
		"we're erroring systematically, the Dune API is down",
	}
	for _, testcase := range testcases {
		t.Run(testcase, func(t *testing.T) {
			t.Skip("not implemented")
		})
	}
}

func TestRunLoopHappyCase(t *testing.T) {
	t.Skip("not implemented")
}
