package common

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLiteralInput(t *testing.T) {
	ctx := newTestingContext()
	defer ctx.Close()
	dirname, err := ioutil.TempDir("/tmp", "commontest")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %q", err)
	}
	defer os.RemoveAll(dirname)

	inputManager := NewInputManager(ctx)
	AplusB, err := NewLiteralInputFactory(
		&LiteralInput{
			Cases: map[string]LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3"},
				"1": {Input: "2 3", ExpectedOutput: "5"},
			},
			Validator: &LiteralValidatorSettings{
				Name: "token-numeric",
			},
		},
		dirname,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	inputManager.Add(AplusB.Hash(), AplusB)
	if _, err := inputManager.Get(AplusB.Hash()); err != nil {
		t.Fatalf("Failed to open input: %q", err)
	}
}
