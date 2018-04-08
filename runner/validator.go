package runner

import (
	"fmt"
	"github.com/omegaup/quark/common"
	"io"
	"math"
	"strconv"
	"strings"
)

// CalculateScore calculates the score of a contestantOutput by comparing it
// with the expectedOutput under the specified validator settings.
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

	var mismatch *TokenMismatch
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
			if !expectedNext && expectedTokenizer.Err() != nil {
				return 0, mismatch, expectedTokenizer.Err()
			}
			if !contestantNext && contestantTokenizer.Err() != nil {
				return 0, mismatch, contestantTokenizer.Err()
			}
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
			return 0, nil, fmt.Errorf("Unknown validator: %q", settings.Name)
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
	if erra != nil || errb != nil {
		return erra != nil && errb != nil
	}

	const SmallestNormal = 2.2250738585072014e-308 // 2**-1022

	diff := math.Abs(bf - af)
	if af == bf {
		return true
	} else if diff <= 1.5*tolerance {
		return true
	} else if af == 0 || bf == 0 || diff < SmallestNormal {
		return diff <= tolerance*SmallestNormal
	}
	return diff/math.Max(math.Abs(af), math.Abs(bf)) <= tolerance
}
