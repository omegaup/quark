package runner

import (
	"bufio"
	"bytes"
	"github.com/omegaup/quark/common"
	"math/big"
	"testing"
)

type VS common.ValidatorSettings

func TestScanNumericTokens(t *testing.T) {
	validatorentries := []struct {
		input  string
		tokens []Token
	}{
		{"hello, world!", []Token{}},
		{"0 0\n 0\n-1", []Token{{"0", 1, 1}, {"0", 1, 3}, {"0", 2, 2}, {"-1", 3, 1}}},
	}
loop:
	for _, vet := range validatorentries {
		tokenizer := NewTokenizer(bytes.NewBufferString(vet.input), IsNumeric)
		for _, expected := range vet.tokens {
			if !tokenizer.Scan() {
				t.Errorf("Expected %v, got EOF", expected)
				continue loop
			}
			got := tokenizer.Token()
			if expected != *got {
				t.Errorf("Expected text %v, got %v", expected, *got)
				continue loop
			}
		}
		if tokenizer.Scan() {
			t.Errorf("Expected EOF, got %v", tokenizer.Token())
			continue
		}
	}
}

func TestValidator(t *testing.T) {
	t1 := 1e-1
	t6 := 1e-6
	validatorentries := []struct {
		expectedScore *big.Rat
		got, expect   string
		settings      VS
	}{
		{big.NewRat(0, 1), "a", "b", VS{Name: "token"}},
		{big.NewRat(1, 1), "a", "a", VS{Name: "token"}},
		{big.NewRat(0, 1), "A", "a", VS{Name: "token"}},
		{big.NewRat(1, 1), "A", "a", VS{Name: "token-caseless"}},
		{big.NewRat(0, 1), "A", "b", VS{Name: "token-caseless"}},
		{big.NewRat(1, 1), "11\x1f\n", "11\n", VS{Name: "token-caseless"}},
		{big.NewRat(0, 1), "1", "2", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1", "1.1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(0, 1), "1", "1.2", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1.15", "1.20", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1.24", "1.20", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(0, 1), "1", "x", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(0, 1), "x", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "x 1", "x 1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1", "x 1 x", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(0, 1), "1", "-1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "0 -1", "0 -1", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "1e99999999", "1e99999999", VS{Name: "token-numeric", Tolerance: &t1}},
		{big.NewRat(1, 1), "0.000002", "0.000003", VS{Name: "token-numeric", Tolerance: &t6}},
		{big.NewRat(0, 1), "a a", "a", VS{Name: "token"}},
		{big.NewRat(0, 1), "a", "a a", VS{Name: "token"}},
		{big.NewRat(1, 2), "0.5", "", VS{Name: "literal"}},
	}
	for _, vet := range validatorentries {
		gotScore, _, err := CalculateScore(
			(*common.ValidatorSettings)(&vet.settings),
			bytes.NewBufferString(vet.expect),
			bytes.NewBufferString(vet.got),
		)
		if err != nil {
			t.Errorf("Error comparing values: %q", err)
			continue
		}
		if gotScore.Cmp(vet.expectedScore) != 0 {
			t.Errorf(
				"CalculateScore(%v) == %s, expected %s",
				vet,
				gotScore.String(),
				vet.expectedScore.String(),
			)
		}
	}

	invalidvalidatorentries := []struct {
		got, expect string
		settings    common.ValidatorSettings
	}{
		{"a", "a", common.ValidatorSettings{}},
		{"", "", common.ValidatorSettings{Name: "literal"}},
		{"x", "", common.ValidatorSettings{Name: "literal"}},
		{"", "", common.ValidatorSettings{Name: "custom"}},
	}
	for _, vet := range invalidvalidatorentries {
		if _, _, err := CalculateScore(
			&vet.settings,
			bytes.NewBufferString(vet.expect),
			bytes.NewBufferString(vet.got),
		); err == nil {
			t.Errorf("Expected to fail, but didn't: %v", vet)
		}
	}
}

func TestHugeTokens(t *testing.T) {
	large := make([]byte, MaxTokenLength-1)
	for idx := range large {
		large[idx] = 'A'
	}
	tokenizer := NewTokenizer(bytes.NewReader(large), IsNonWhitespace)
	if !tokenizer.Scan() {
		t.Errorf("Expected to scan a token. Err: %v", tokenizer.Err())
	}

	large = make([]byte, MaxTokenLength)
	for idx := range large {
		large[idx] = 'A'
	}
	tokenizer = NewTokenizer(bytes.NewReader(large), IsNonWhitespace)
	if tokenizer.Scan() {
		t.Errorf("Expected to fail scanning a token")
	}
	if bufio.ErrTooLong != tokenizer.Err() {
		t.Errorf("Expected %v, got %v", bufio.ErrTooLong, tokenizer.Err())
	}
}
