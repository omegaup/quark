package runner

import (
	"bytes"
	"github.com/lhchavez/quark/common"
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
	t1 := 0.1
	validatorentries := []struct {
		expectedScore float64
		got, expect   string
		settings      VS
	}{
		{0.0, "a", "b", VS{Name: "token"}},
		{1.0, "a", "a", VS{Name: "token"}},
		{0.0, "A", "a", VS{Name: "token"}},
		{1.0, "A", "a", VS{Name: "token-caseless"}},
		{0.0, "A", "b", VS{Name: "token-caseless"}},
		{1.0, "11\x1f\n", "11\n", VS{Name: "token-caseless"}},
		{0.0, "1", "2", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1", "1.1", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "1", "1.2", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "1", "x", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "x", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "x 1", "x 1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1", "x 1 x", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "1", "-1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "0 -1", "0 -1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1e99999999", "1e99999999", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "a a", "a", VS{Name: "token"}},
		{0.0, "a", "a a", VS{Name: "token"}},
		{0.5, "0.5", "", VS{Name: "literal"}},
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
		if gotScore != vet.expectedScore {
			t.Errorf(
				"CalculateScore(%v) == %f, expected %f",
				vet,
				gotScore,
				vet.expectedScore,
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
