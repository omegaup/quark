package grader

import (
	"encoding/base64"
	"github.com/omegaup/quark/common"
	"io/ioutil"
	"math/big"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unicode"
)

func TestPreloadInputs(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	cachePath := path.Join(ctx.Config.Grader.RuntimePath, "cache")
	files := []struct {
		filename, contents string
	}{
		{
			"00/00000000000000000000000000000000000000.tar.gz",
			"",
		},
		{
			"00/00000000000000000000000000000000000000.tar.gz.len",
			"MA==",
		},
		{
			"00/00000000000000000000000000000000000000.tar.gz.sha1",
			`MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMCAqMDAwMDAwMDAwMDAwMDAw
			MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMC50YXIuZ3o=`,
		},
		{
			"00/00000000000000000000000000000000000001.tar.gz",
			"",
		},
		{
			"00/00000000000000000000000000000000000001.tar.gz.len",
			"MA==",
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad.tar.gz",
			`H4sIAFBMXVYAA+2WzU+DMBTAd93+CsJZWUspEK/Gmx+HLXowHppZZx0UA2VqFv53X1HYXDKXmDCi
			vt+B0r6Pvva1D5QeD7qGABHntqURJ5ttw9d3ShllA4d3HhlQFkbkjjNYinkutNmpt0/+S1F6TDyl
			O53DJjUMgm/yz7bzH0WQf9JpVJ/88/xTx+87BKRHstJ0/gH4Qf3nNMD6fwhs/okHzw7n2F//6Vb+
			fYb1/zCwvgNAeqWQxig9L7ynIuvqL3Df/Q/hZ5/6oR/5Aec+1ALKOOF4/w/BauQ47qkoZOGeOLej
			4XC17q3cS5FKeHOJe+S4N1LNHw10qUeqOxjYLQWnoGBdn6tUGevNTgT9s1eTixuRJFNVG5NaDQQX
			Ms3yt1odhsOIkjgOg0Z6VZrn0jRSGrJ4LVrKHPw1PlsP9mQ1OhMjZovWmgQxj8JWuGnFNoyuRaLu
			hcnybQUQVx+rmyTZCww+iKSQMDJc29gVtxtksoXUx7pMZa5mdrOmWQJB61m9Ax5ch2pUjfo+CgiC
			IAiCIAiCIAiCIAiCIAiC/BHeAU4V1PQAKAAA`,
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad.tar.gz.sha1",
			`YTMyZjBmMDE4NDQxZGY3MWVmYzhkMWM2YTU1MzkxMzU0YThkNDg5NyAqNGJiYTYxYjU0OTlhN2E1
			MTFlYjUxNTU5NGYzMjkzYTg3NDE1MTZhZC50YXIuZ3oK`,
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad.tar.gz.len",
			"Mzk5",
		},
	}
	for _, ft := range files {
		decoded, err := base64.StdEncoding.DecodeString(
			strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return -1
				}
				return r
			}, ft.contents),
		)
		if err != nil {
			t.Fatalf("Failed to decode base64-encoded string: %q", err)
		}
		if err := os.MkdirAll(
			path.Join(cachePath, path.Dir(ft.filename)),
			0755,
		); err != nil {
			t.Fatalf("Failed to create directory: %q", err)
		}
		if err := ioutil.WriteFile(
			path.Join(cachePath, ft.filename),
			decoded,
			0644,
		); err != nil {
			t.Fatalf("Failed to write file: %q", err)
		}
	}
	var AplusBHash string
	{
		AplusB, err := common.NewLiteralInputFactory(
			&common.LiteralInput{
				Cases: map[string]common.LiteralCaseSettings{
					"0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
					"1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(1, 1)},
				},
				Validator: &common.LiteralValidatorSettings{
					Name: "token-numeric",
				},
			},
			ctx.Config.Grader.RuntimePath,
			common.LiteralPersistGrader,
		)
		if err != nil {
			t.Fatalf("Failed to create InputFactory: %q", err)
		}
		inputManager := common.NewInputManager(&ctx.Context)
		AplusBInput, err := inputManager.Add(AplusB.Hash(), AplusB)
		if err != nil {
			t.Fatalf("Failed to create Input: %q", err)
		}
		AplusBHash = AplusBInput.Hash()
		if err = AplusBInput.Persist(); err != nil {
			t.Fatalf("Failed to persist Input: %q", err)
		}
		AplusBInput.Release(AplusBInput)
	}
	ctx.InputManager.PreloadInputs(
		cachePath,
		NewCachedInputFactory(cachePath),
		&sync.Mutex{},
	)

	hashentries := []struct {
		hash  string
		valid bool
	}{
		{"0000000000000000000000000000000000000000", false},
		{"0000000000000000000000000000000000000001", false},
		{"4bba61b5499a7a511eb515594f3293a8741516ad", true},
		{AplusBHash, true},
	}
	for _, het := range hashentries {
		input, err := ctx.InputManager.Get(het.hash)
		if input != nil {
			defer input.Release(input)
		}
		if het.valid {
			if err != nil {
				t.Errorf("InputManager.Get(%q) == %q, want nil", het.hash, err)
			}
		} else {
			if err == nil {
				t.Errorf("InputManager.Get(%q) == %q, want !nil", het.hash, err)
			}
		}
	}
}

func TestTransmitInput(t *testing.T) {
	ctx, err := newGraderContext()
	if err != nil {
		t.Fatalf("GraderContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Grader.RuntimePath)
	}

	input, err := ctx.InputManager.Add(
		kHeadCommit,
		NewInputFactory("test", &ctx.Config),
	)
	if err != nil {
		t.Fatalf("Failed to get the input: %q", err)
	}
	defer input.Release(input)
	if err := input.Verify(); err != nil {
		t.Fatalf("Failed to verify the input: %q", err)
	}

	graderInput := input.(*Input)
	w := httptest.NewRecorder()
	if err := graderInput.Transmit(w); err != nil {
		t.Fatalf("Failed to transmit input: %q", err)
	}
	headers := w.Header()

	headerentries := []struct {
		name, value string
	}{
		{"Content-Type", "application/x-gzip"},
		{"Content-Sha1", "d80feecc2f19607c592bdb6a1d05aa5a234b8498"},
	}
	for _, het := range headerentries {
		if !reflect.DeepEqual(headers[het.name], []string{het.value}) {
			t.Fatalf("%s == %q, want %q", het.name, headers[het.name], het.value)
		}
	}
}
