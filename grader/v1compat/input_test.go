package v1compat

import (
	"math/big"
	"testing"
)

func TestParseTestplan(t *testing.T) {
	rawCaseWeights, err := parseTestplan("1 10.5\n2 10.5")
	if err != nil {
		t.Fatalf("Testplan parsing failed with %q", err)
	}
	expectedCaseWeights := map[string]*big.Rat{
		"1": big.NewRat(21, 2),
		"2": big.NewRat(21, 2),
	}
	for name, expectedWeight := range expectedCaseWeights {
		weight, ok := rawCaseWeights[name]
		if !ok {
			t.Errorf("rawCaseWeights[%q] is missing, want %s", name, expectedWeight.String())
		}
		if expectedWeight.Cmp(weight) != 0 {
			t.Errorf("rawCaseWeights[%q] == %s, want %s", name, weight.String(), expectedWeight.String())
		}
	}
}
