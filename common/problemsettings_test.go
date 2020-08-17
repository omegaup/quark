package common

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestCaseWeightMappingSymmetricDiff(t *testing.T) {
	caseWeightMapping := CaseWeightMapping(map[string]map[string]*big.Rat{
		"group": {
			"group.case": big.NewRat(1, 1),
		},
	})
	emptyCaseMapping := NewCaseWeightMapping()

	err := caseWeightMapping.SymmetricDiff(emptyCaseMapping, "left")
	expectedErrorString := "left missing case \"group.case\""
	if err == nil {
		t.Errorf("Expected an error, got nil")
	} else if expectedErrorString != err.Error() {
		t.Errorf("expected %v, got %v", expectedErrorString, err)
	}
}

func TestCaseWeightMappingToGroupSettings(t *testing.T) {
	caseWeightMapping := CaseWeightMapping(map[string]map[string]*big.Rat{
		"group2": {
			"group2.case2": big.NewRat(1, 1),
			"group2.case1": big.NewRat(1, 1),
		},
		"group1": {
			"group1.case2": big.NewRat(1, 1),
			"group1.case1": big.NewRat(1, 1),
		},
	})
	expectedGroupSettings := []GroupSettings{
		{
			Name: "group1",
			Cases: []CaseSettings{
				{
					Name:   "group1.case1",
					Weight: big.NewRat(1, 1),
				},
				{
					Name:   "group1.case2",
					Weight: big.NewRat(1, 1),
				},
			},
		},
		{
			Name: "group2",
			Cases: []CaseSettings{
				{
					Name:   "group2.case1",
					Weight: big.NewRat(1, 1),
				},
				{
					Name:   "group2.case2",
					Weight: big.NewRat(1, 1),
				},
			},
		},
	}

	groupSettings := caseWeightMapping.ToGroupSettings()
	if !reflect.DeepEqual(expectedGroupSettings, groupSettings) {
		t.Errorf("expected %v, got %v", expectedGroupSettings, groupSettings)
	}
}

func TestCaseWeightMappingParseTestplan(t *testing.T) {
	caseWeightMapping, err := NewCaseWeightMappingFromTestplan(
		strings.NewReader(`
			group1.case1 1
			group1.case2 1

			# comment
			group2.case2 1
			group2.case1 1
		`),
	)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	expectedGroupSettings := []GroupSettings{
		{
			Name: "group1",
			Cases: []CaseSettings{
				{
					Name:   "group1.case1",
					Weight: big.NewRat(1, 1),
				},
				{
					Name:   "group1.case2",
					Weight: big.NewRat(1, 1),
				},
			},
		},
		{
			Name: "group2",
			Cases: []CaseSettings{
				{
					Name:   "group2.case1",
					Weight: big.NewRat(1, 1),
				},
				{
					Name:   "group2.case2",
					Weight: big.NewRat(1, 1),
				},
			},
		},
	}

	groupSettings := caseWeightMapping.ToGroupSettings()
	if !reflect.DeepEqual(expectedGroupSettings, groupSettings) {
		t.Errorf("expected %v, got %v", expectedGroupSettings, groupSettings)
	}
}
