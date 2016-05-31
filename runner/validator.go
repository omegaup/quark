package runner

import (
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
	expectedOutput, contestantOutput io.Reader,
) (float64, *TokenMismatch, error) {
	scanFunc := IsNonWhitespace
	if settings.Name == "token-numeric" {
		scanFunc = IsNumeric
	}

	contestantTokenizer := NewTokenizer(contestantOutput, scanFunc)
	if settings.Name == "literal" || settings.Name == "custom" {
		if !contestantTokenizer.Scan() {
			return 0, nil, io.ErrUnexpectedEOF
		}
		value, err := strconv.ParseFloat(contestantTokenizer.Token().Text, 64)
		return math.Max(0, math.Min(1, value)), nil, err
	}

	expectedTokenizer := NewTokenizer(expectedOutput, scanFunc)

	var mismatch *TokenMismatch = nil
	for mismatch == nil {
		expectedNext := expectedTokenizer.Scan()
		contestantNext := contestantTokenizer.Scan()
		if expectedNext != contestantNext {
			mismatch = &TokenMismatch{}
			if expectedNext {
				mismatch.Expected = expectedTokenizer.Token()
			}
			if contestantNext {
				mismatch.Contestant = contestantTokenizer.Token()
			}
		}
		if !expectedNext || !contestantNext {
			break
		}
		expectedToken := expectedTokenizer.Token()
		contestantToken := contestantTokenizer.Token()
		correct := true
		switch settings.Name {
		case "token":
			correct = tokenEquals(expectedToken.Text, contestantToken.Text)
		case "token-caseless":
			correct = tokenCaselessEquals(expectedToken.Text, contestantToken.Text)
		case "token-numeric":
			correct = tokenNumericEquals(
				expectedToken.Text,
				contestantToken.Text,
				*settings.Tolerance,
			)
		default:
			return 0, nil, errors.New(fmt.Sprintf("Unknown validator: %q", settings.Name))
		}
		if !correct {
			mismatch = &TokenMismatch{
				Contestant: contestantToken,
				Expected:   expectedToken,
			}
		}
	}
	if mismatch != nil {
		return 0.0, mismatch, nil
	}
	return 1.0, nil, nil
}

func tokenEquals(a, b string) bool {
	return a == b
}

func tokenCaselessEquals(a, b string) bool {
	return strings.EqualFold(a, b)
}

func tokenNumericEquals(a, b string, tolerance float64) bool {
	af, erra := strconv.ParseFloat(a, 64)
	bf, errb := strconv.ParseFloat(b, 64)
	if erra == nil && errb == nil {
		return math.Abs(af-bf) <= math.Abs(af)*tolerance
	}
	return erra != nil && errb != nil
}
