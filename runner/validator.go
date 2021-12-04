package runner

import (
	"fmt"
	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// CalculateScore calculates the score of a contestantOutput by comparing it
// with the expectedOutput under the specified validator settings.
func CalculateScore(
	settings *common.ValidatorSettings,
	expectedOutput, contestantOutput io.Reader,
) (*big.Rat, *TokenMismatch, error) {
	scanFunc := IsNonWhitespace
	if settings.Name == common.ValidatorNameTokenNumeric {
		scanFunc = IsNumeric
	}

	contestantTokenizer := NewTokenizer(contestantOutput, scanFunc)
	if settings.Name == common.ValidatorNameLiteral || settings.Name == common.ValidatorNameCustom {
		if !contestantTokenizer.Scan() {
			return &big.Rat{}, nil, io.ErrUnexpectedEOF
		}
		value, err := base.ParseRational(contestantTokenizer.Token().Text)
		if err != nil {
			return &big.Rat{}, nil, err
		}
		return ratClamp(value, &big.Rat{}, big.NewRat(1, 1)), nil, nil
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
				return &big.Rat{}, mismatch, expectedTokenizer.Err()
			}
			if !contestantNext && contestantTokenizer.Err() != nil {
				return &big.Rat{}, mismatch, contestantTokenizer.Err()
			}
			break
		}
		expectedToken := expectedTokenizer.Token()
		contestantToken := contestantTokenizer.Token()
		correct := true
		switch settings.Name {
		case common.ValidatorNameToken:
			correct = tokenEquals(expectedToken.Text, contestantToken.Text)
		case common.ValidatorNameTokenCaseless:
			correct = tokenCaselessEquals(expectedToken.Text, contestantToken.Text)
		case common.ValidatorNameTokenNumeric:
			tolerance := common.DefaultValidatorTolerance
			if settings.Tolerance != nil {
				tolerance = *settings.Tolerance
			}
			correct = tokenNumericEquals(
				expectedToken.Text,
				contestantToken.Text,
				tolerance,
			)
		default:
			return &big.Rat{}, nil, fmt.Errorf("Unknown validator: %q", settings.Name)
		}
		if !correct {
			mismatch = &TokenMismatch{
				Contestant: contestantToken,
				Expected:   expectedToken,
			}
		}
	}
	if mismatch != nil {
		return &big.Rat{}, mismatch, nil
	}
	return big.NewRat(1, 1), nil, nil
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

func ratClamp(val, min, max *big.Rat) *big.Rat {
	if val.Cmp(min) <= 0 {
		return min
	}
	if val.Cmp(max) >= 0 {
		return max
	}
	return val
}
