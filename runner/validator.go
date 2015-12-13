package runner

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"math"
	"strconv"
	"strings"
)

func CalculateScore(
	settings *common.ValidatorSettings,
	contestantOutput, expectedOutput io.Reader,
) (float64, error) {
	contestantScanner := bufio.NewScanner(contestantOutput)
	contestantScanner.Split(bufio.ScanWords)
	if settings.Name == "literal" || settings.Name == "custom" {
		if !contestantScanner.Scan() {
			return 0, io.ErrUnexpectedEOF
		}
		value, err := strconv.ParseFloat(contestantScanner.Text(), 64)
		return math.Max(0, math.Min(1, value)), err
	}

	expectedScanner := bufio.NewScanner(expectedOutput)
	expectedScanner.Split(bufio.ScanWords)

	correct := true
	for correct {
		expectedNext := expectedScanner.Scan()
		contestantNext := contestantScanner.Scan()
		if expectedNext != contestantNext {
			correct = false
		}
		if !expectedNext {
			break
		}
		switch settings.Name {
		case "token":
			correct = token(expectedScanner.Text(), contestantScanner.Text())
		case "token-caseless":
			correct = tokenCaseless(expectedScanner.Text(), contestantScanner.Text())
		case "token-numeric":
			correct = tokenNumeric(
				expectedScanner.Text(),
				contestantScanner.Text(),
				*settings.Tolerance,
			)
		default:
			return 0, errors.New(fmt.Sprintf("Unknown validator: %q", settings.Name))
		}
	}
	if !correct {
		return 0.0, nil
	}
	return 1.0, nil
}

func token(a, b string) bool {
	return a == b
}

func tokenCaseless(a, b string) bool {
	return strings.EqualFold(a, b)
}

func tokenNumeric(a, b string, tolerance float64) bool {
	af, erra := strconv.ParseFloat(a, 64)
	bf, errb := strconv.ParseFloat(b, 64)
	if erra == nil && errb == nil {
		return math.Abs(af-bf) <= af*tolerance
	}
	return erra != nil && errb != nil
}
