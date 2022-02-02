package common

import (
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
)

func TestLiteralInput(t *testing.T) {
	ctx := newTestingContext()
	defer ctx.Close()
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temp directory: %q", err)
	}
	defer os.RemoveAll(dirname)

	inputManager := NewInputManager(ctx)
	AplusB, err := NewLiteralInputFactory(
		&LiteralInput{
			Cases: map[string]*LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
				"1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(1, 1)},
			},
			Validator: &LiteralValidatorSettings{
				Name: ValidatorNameTokenNumeric,
			},
		},
		dirname,
		LiteralPersistNone,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
	if err != nil {
		t.Fatalf("Failed to open input: %q", err)
	}
	defer inputRef.Release()
}

func TestLiteralInputWithExpectedStderr(t *testing.T) {
	ctx := newTestingContext()
	defer ctx.Close()
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temp directory: %q", err)
	}
	defer os.RemoveAll(dirname)

	inputManager := NewInputManager(ctx)
	AplusB, err := NewLiteralInputFactory(
		&LiteralInput{
			Cases: map[string]*LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3", ExpectedValidatorStderr: "err", Weight: big.NewRat(1, 1)},
				"1": {Input: "2 3", ExpectedOutput: "5", ExpectedValidatorStderr: "err", Weight: big.NewRat(1, 1)},
			},
			Validator: &LiteralValidatorSettings{
				Name: ValidatorNameTokenNumeric,
			},
		},
		dirname,
		LiteralPersistNone,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	for _, caseName := range []string{"0", "1"} {
		value, ok := (*AplusB.input.files)[fmt.Sprintf("cases/%s.expected-failure", caseName)]
		if !ok {
			t.Fatalf("Missing .expected-failure file for case: %s", caseName)
		}
		if string(value) != "err" {
			t.Fatalf("Unexpected contents in .expected-failure file for case: %s", caseName)
		}
	}
	inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
	if err != nil {
		t.Fatalf("Failed to open input: %q", err)
	}
	defer inputRef.Release()
}
