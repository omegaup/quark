package v1compat

import (
	"testing"
)

func TestParseTestplan(t *testing.T) {
	rawCaseWeights, err := parseTestplan("1 10.5\n2 10.5")
	if err != nil {
		t.Fatalf("Testplan parsing failed with %q", err)
	}
	expectedCaseWeights := map[string]float64{
		"1": 10.5,
		"2": 10.5,
	}
	for name, expectedWeight := range expectedCaseWeights {
		weight, ok := rawCaseWeights[name]
		if !ok {
			t.Errorf("rawCaseWeights[%q] is missing, want %q", name, expectedWeight)
		}
		if expectedWeight != weight {
			t.Errorf("rawCaseWeights[%q] == %q, want %q", name, weight, expectedWeight)
		}
	}
}
