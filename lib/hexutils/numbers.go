package hexutils

import (
	"math/big"
	"strconv"

	"github.com/go-errors/errors"
)

func IntFromHex(hexNumber string) (int64, error) {
	// Empty string is OK
	if len(hexNumber) == 0 {
		return 0, nil
	}
	if len(hexNumber) < 2 || hexNumber[:2] != "0x" {
		return 0, errors.Errorf("couldn't parse '%s' as number, must start with '0x'", hexNumber)
	}
	n, err := strconv.ParseInt(hexNumber[2:], 16, 64)
	if err != nil {
		return 0, errors.Errorf("failed to parse '%s' as int: %w", hexNumber, err)
	}
	return n, nil
}

func BigIntFromHex(hexNumber string) (string, error) {
	// Empty string is OK
	if len(hexNumber) == 0 {
		return "", nil
	}
	if len(hexNumber) < 2 || hexNumber[:2] != "0x" {
		return "", errors.Errorf("couldn't parse '%s' as number, must start with '0x'", hexNumber)
	}
	n := &big.Int{}
	if _, ok := n.SetString(hexNumber[2:], 16); !ok {
		return "", errors.Errorf("failed to parse '%s' as number", hexNumber)
	}
	return n.Text(10), nil
}
