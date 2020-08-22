package ci

import (
	"math/big"
	"reflect"
	"strings"
	"testing"

	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
)

func TestReportTestSetResult(t *testing.T) {
	for _, tt := range []struct {
		name          string
		reportTest    ReportTest
		result        *runner.RunResult
		expectedState State
	}{
		{
			"expected explicit verdict",
			ReportTest{SolutionSetting: &common.SolutionSettings{Verdict: "WA"}},
			&runner.RunResult{Verdict: "WA"},
			StatePassed,
		},
		{
			"unexpected explicit verdict",
			ReportTest{SolutionSetting: &common.SolutionSettings{Verdict: "AC"}},
			&runner.RunResult{Verdict: "WA"},
			StateFailed,
		},
		{
			"expected implicit verdict",
			ReportTest{},
			&runner.RunResult{Verdict: "AC"},
			StatePassed,
		},
		{
			"unexpected implicit verdict",
			ReportTest{},
			&runner.RunResult{Verdict: "WA"},
			StateFailed,
		},
		{
			"expected score",
			ReportTest{
				SolutionSetting: &common.SolutionSettings{
					ScoreRange: &common.ScoreRange{Min: big.NewRat(1, 1), Max: big.NewRat(1, 1)},
				},
			},
			&runner.RunResult{Score: big.NewRat(1, 1)},
			StatePassed,
		},
		{
			"unexpected score (below)",
			ReportTest{
				SolutionSetting: &common.SolutionSettings{
					ScoreRange: &common.ScoreRange{Min: big.NewRat(1, 1), Max: big.NewRat(1, 1)},
				},
			},
			&runner.RunResult{Score: big.NewRat(0, 1)},
			StateFailed,
		},
		{
			"unexpected score (above)",
			ReportTest{
				SolutionSetting: &common.SolutionSettings{
					ScoreRange: &common.ScoreRange{Min: big.NewRat(0, 1), Max: big.NewRat(0, 1)},
				},
			},
			&runner.RunResult{Score: big.NewRat(1, 1)},
			StateFailed,
		},
		{
			"expected score and verdict",
			ReportTest{
				SolutionSetting: &common.SolutionSettings{
					Verdict:    "PA",
					ScoreRange: &common.ScoreRange{Min: big.NewRat(1, 2), Max: big.NewRat(1, 2)},
				},
			},
			&runner.RunResult{Verdict: "PA", Score: big.NewRat(1, 2)},
			StatePassed,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.reportTest.SetResult(tt.result)
			if tt.expectedState != tt.reportTest.State {
				t.Errorf("expected ReportTest.State = %v, got %v", tt.expectedState, tt.reportTest.State)
			}
		})
	}
}

func TestReportUpdateState(t *testing.T) {
	for _, tt := range []struct {
		name          string
		report        Report
		expectedState State
	}{
		{
			"empty tests",
			Report{},
			StatePassed,
		},
		{
			"passed",
			Report{
				Tests: []*ReportTest{
					{State: StatePassed},
				},
			},
			StatePassed,
		},
		{
			"failed",
			Report{
				Tests: []*ReportTest{
					{State: StatePassed},
					{State: StateFailed},
				},
			},
			StateFailed,
		},
		{
			"error",
			Report{
				Tests: []*ReportTest{
					{State: StatePassed},
					{State: StateFailed},
					{State: StateError},
				},
			},
			StateError,
		},
		{
			"skipped",
			Report{
				Tests: []*ReportTest{
					{State: StatePassed},
					{State: StateFailed},
					{State: StateError},
					{State: StateSkipped},
				},
			},
			StateSkipped,
		},
		{
			"running",
			Report{
				Tests: []*ReportTest{
					{State: StatePassed},
					{State: StateFailed},
					{State: StateError},
					{State: StateSkipped},
					{State: StateRunning},
				},
			},
			StateRunning,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.report.State = StatePassed
			tt.report.UpdateState()

			if tt.expectedState != tt.report.State {
				t.Errorf("expected Report.State = %v, got %v", tt.expectedState, tt.report.State)
			}
		})
	}
}

func TestNewRunConfig(t *testing.T) {
	for _, tt := range []struct {
		name              string
		problemFiles      common.ProblemFiles
		expectedRunConfig *RunConfig
		errorSubstring    string
	}{
		{
			"missing tests.json",
			common.NewProblemFilesFromMap(
				map[string]string{},
				":memory:",
			),
			nil,
			"",
		},
		{
			"missing settings.json",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": "{}",
				},
				":memory:",
			),
			nil,
			"",
		},
		{
			"empty files",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": "{}",
					"settings.json":    "{}",
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{},
				TestConfigs:   nil,
			},
			"",
		},
		{
			"single solution, missing extension",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac"
							}
						]
					}`,
					"settings.json": "{}",
				},
				":memory:",
			),
			nil,
			"failed to get solution language",
		},
		{
			"single solution, missing solution file",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"settings.json": "{}",
				},
				":memory:",
			),
			nil,
			"\"tests/ac.py\" in \":memory:\": file does not exist",
		},
		{
			"single solution",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py":   "print(3)",
					"settings.json": "{}",
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{
					Solutions: []common.SolutionSettings{
						{Filename: "ac.py"},
					},
				},
				TestConfigs: []*TestConfig{
					{
						Test: &ReportTest{
							Type:            "solutions",
							Filename:        "ac.py",
							SolutionSetting: &common.SolutionSettings{Filename: "ac.py"},
						},
						Source:   "print(3)",
						Language: "py",
						Input: &common.LiteralInput{
							Cases:     map[string]*common.LiteralCaseSettings{},
							Limits:    &common.DefaultLimits,
							Validator: &common.LiteralValidatorSettings{},
						},
					},
				},
			},
			"",
		},
		{
			"input validator, missing extension",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"inputs": {
							"filename": "validator"
						}
					}`,
					"settings.json": "{}",
				},
				":memory:",
			),
			nil,
			"failed to get input validator language",
		},
		{
			"input validator, missing file",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"inputs": {
							"filename": "validator.py"
						}
					}`,
					"settings.json": "{}",
				},
				":memory:",
			),
			nil,
			"\"tests/validator.py\" in \":memory:\": file does not exist",
		},
		{
			"input validator",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"inputs": {
							"filename": "validator.py"
						}
					}`,
					"tests/validator.py": "print(3)",
					"settings.json":      "{}",
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{
					InputsValidator: &common.InputsValidatorSettings{Filename: "validator.py"},
				},
				TestConfigs: []*TestConfig{
					{
						Test: &ReportTest{
							Type:                   "inputs",
							Filename:               "validator.py",
							InputsValidatorSetting: &common.InputsValidatorSettings{Filename: "validator.py"},
						},
						Source:   CopyStdinToStdoutSource,
						Language: "cpp11",
						Input: &common.LiteralInput{
							Cases: map[string]*common.LiteralCaseSettings{},
							Validator: &common.LiteralValidatorSettings{
								Name: "custom",
								CustomValidator: &common.LiteralCustomValidatorSettings{
									Source:   "print(3)",
									Language: "py",
								},
							},
						},
					},
				},
			},
			"\"tests/validator.py\" in \":memory:\": file does not exist",
		},
		{
			"explicit cases, missing .in",
			common.NewProblemFilesFromMap(
				map[string]string{
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py": "print(3)",
					"settings.json": `{
						"Cases": [
							{
								"Name": "0",
								"Cases": [
									{
										"Name": "0",
										"Weight": 1
									}
								]
							}
						]
					}`,
				},
				":memory:",
			),
			nil,
			"open \"cases/0.in\"",
		},
		{
			"explicit cases, missing .out",
			common.NewProblemFilesFromMap(
				map[string]string{
					"cases/0.in": "1 2",
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py": "print(3)",
					"settings.json": `{
						"Cases": [
							{
								"Name": "0",
								"Cases": [
									{
										"Name": "0",
										"Weight": 1
									}
								]
							}
						]
					}`,
				},
				":memory:",
			),
			nil,
			"open \"cases/0.out\"",
		},
		{
			"explicit cases",
			common.NewProblemFilesFromMap(
				map[string]string{
					"cases/0.in":  "1 2",
					"cases/0.out": "3",
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py": "print(3)",
					"settings.json": `{
						"Cases": [
							{
								"Name": "0",
								"Cases": [
									{
										"Name": "0",
										"Weight": 1
									}
								]
							}
						]
					}`,
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{
					Solutions: []common.SolutionSettings{
						{Filename: "ac.py"},
					},
				},
				TestConfigs: []*TestConfig{
					{
						Test: &ReportTest{
							Type:            "solutions",
							Filename:        "ac.py",
							SolutionSetting: &common.SolutionSettings{Filename: "ac.py"},
						},
						Source:   "print(3)",
						Language: "py",
						Input: &common.LiteralInput{
							Cases: map[string]*common.LiteralCaseSettings{
								"0": {
									Input:          "1 2",
									ExpectedOutput: "3",
									Weight:         big.NewRat(1, 1),
								},
							},
							Limits:    &common.DefaultLimits,
							Validator: &common.LiteralValidatorSettings{},
						},
					},
				},
			},
			"",
		},
		{
			"implicit cases, missing .out",
			common.NewProblemFilesFromMap(
				map[string]string{
					"cases/0.in": "1 2",
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py":   "print(3)",
					"settings.json": "{}",
				},
				":memory:",
			),
			nil,
			"open \"cases/0.out\"",
		},
		{
			"implicit cases",
			common.NewProblemFilesFromMap(
				map[string]string{
					"cases/0.in":  "1 2",
					"cases/0.out": "3",
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py":   "print(3)",
					"settings.json": "{}",
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{
					Solutions: []common.SolutionSettings{
						{Filename: "ac.py"},
					},
				},
				TestConfigs: []*TestConfig{
					{
						Test: &ReportTest{
							Type:            "solutions",
							Filename:        "ac.py",
							SolutionSetting: &common.SolutionSettings{Filename: "ac.py"},
						},
						Source:   "print(3)",
						Language: "py",
						Input: &common.LiteralInput{
							Cases: map[string]*common.LiteralCaseSettings{
								"0": {
									Input:          "1 2",
									ExpectedOutput: "3",
									Weight:         big.NewRat(1, 1),
								},
							},
							Limits:    &common.DefaultLimits,
							Validator: &common.LiteralValidatorSettings{},
						},
					},
				},
			},
			"",
		},
		{
			"implicit cases, testplan",
			common.NewProblemFilesFromMap(
				map[string]string{
					"cases/0.in":  "1 2",
					"cases/0.out": "3",
					"cases/1.in":  "1 2",
					"cases/1.out": "3",
					"testplan":    "0 1\n1 2\n",
					"tests/tests.json": `{
						"solutions": [
							{
								"filename": "ac.py"
							}
						]
					}`,
					"tests/ac.py":   "print(3)",
					"settings.json": "{}",
				},
				":memory:",
			),
			&RunConfig{
				TestsSettings: common.TestsSettings{
					Solutions: []common.SolutionSettings{
						{Filename: "ac.py"},
					},
				},
				TestConfigs: []*TestConfig{
					{
						Test: &ReportTest{
							Type:            "solutions",
							Filename:        "ac.py",
							SolutionSetting: &common.SolutionSettings{Filename: "ac.py"},
						},
						Source:   "print(3)",
						Language: "py",
						Input: &common.LiteralInput{
							Cases: map[string]*common.LiteralCaseSettings{
								"0": {
									Input:          "1 2",
									ExpectedOutput: "3",
									Weight:         big.NewRat(1, 1),
								},
								"1": {
									Input:          "1 2",
									ExpectedOutput: "3",
									Weight:         big.NewRat(2, 1),
								},
							},
							Limits:    &common.DefaultLimits,
							Validator: &common.LiteralValidatorSettings{},
						},
					},
				},
			},
			"",
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			runConfig, err := NewRunConfig(tt.problemFiles)
			if err != nil {
				if base.HasErrorCategory(err, ErrSkipped) && tt.expectedRunConfig == nil {
					// Everything is okay.
					return
				}
				if tt.errorSubstring != "" && strings.Contains(err.Error(), tt.errorSubstring) {
					// Everything is okay.
					return
				}
				t.Fatalf("failed to parse run config: %v", err)
			} else if tt.expectedRunConfig == nil {
				t.Fatalf("expected parsing to be skipped, got: %v", runConfig)
			}

			if !reflect.DeepEqual(tt.expectedRunConfig, runConfig) {
				t.Errorf("expected RunConfig = %+v, got %+v", tt.expectedRunConfig, runConfig)
			}
		})
	}
}
