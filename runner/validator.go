package runner

import (
	"bufio"
	"errors"
	"github.com/lhchavez/quark/common"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

func CalculateScore(ctx *common.Context,
	settings *common.ValidatorSettings, caseData *common.CaseSettings,
	expectedOutput, contestantOutput string) (float64, error) {
	if settings.Name == "custom" {
		return 0, errors.New("Not supported")
	}
	contestantFd, err := os.Open(contestantOutput)
	if err != nil {
		return 0, err
	}
	defer contestantFd.Close()
	contestantScanner := bufio.NewScanner(contestantFd)
	contestantScanner.Split(bufio.ScanWords)
	if settings.Name == "literal" {
		if !contestantScanner.Scan() {
			return 0, io.ErrUnexpectedEOF
		}
		value, err := strconv.ParseFloat(contestantScanner.Text(), 64)
		return math.Max(0, math.Min(1, value)), err
	}

	expectedFd, err := os.Open(expectedOutput)
	if err != nil {
		return 0, err
	}
	defer expectedFd.Close()
	expectedScanner := bufio.NewScanner(expectedFd)
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
			correct = tokenNumeric(expectedScanner.Text(), contestantScanner.Text(),
				*settings.Tolerance)
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
	if erra != errb {
		return false
	}
	if erra != nil && errb != nil {
		return true
	}
	return math.Abs(af-bf) <= af*tolerance
}
