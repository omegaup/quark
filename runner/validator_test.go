package runner

import (
	"bytes"
	"github.com/lhchavez/quark/common"
	"testing"
)

type VS common.ValidatorSettings

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
		{0.0, "1", "2", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1", "1.1", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "1", "1.2", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "1", "x", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "x", "1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "x 1", "x 1", VS{Name: "token-numeric", Tolerance: &t1}},
		{1.0, "1e99999999", "1e99999999", VS{Name: "token-numeric", Tolerance: &t1}},
		{0.0, "a a", "a", VS{Name: "token"}},
		{0.0, "a", "a a", VS{Name: "token"}},
		{0.5, "0.5", "", VS{Name: "literal"}},
	}
	for _, vet := range validatorentries {
		gotScore, err := CalculateScore(
			(*common.ValidatorSettings)(&vet.settings),
			bytes.NewBufferString(vet.got),
			bytes.NewBufferString(vet.expect),
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
		if _, err := CalculateScore(
			&vet.settings,
			bytes.NewBufferString(vet.got),
			bytes.NewBufferString(vet.expect),
		); err == nil {
			t.Errorf("Expected to fail, but didn't: %v", vet)
		}
	}
}
