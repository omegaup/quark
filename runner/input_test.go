package runner

import (
	"encoding/base64"
	"github.com/omegaup/quark/common"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"unicode"
)

func mustDecode(b64 string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(
		strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, b64),
	)
	if err != nil {
		panic(err)
	}
	return decoded
}

func TestPreloadInputs(t *testing.T) {
	ctx, err := newRunnerContext(t)
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Runner.RuntimePath)
	}

	inputManager := common.NewInputManager(ctx)

	inputPath := path.Join(ctx.Config.Runner.RuntimePath, "input")
	// Setting up files.
	dirs := []string{
		"00/00000000000000000000000000000000000000",
		"00/00000000000000000000000000000000000001",
		"00/00000000000000000000000000000000000002",
		"00/00000000000000000000000000000000000003",
		"00/00000000000000000000000000000000000004",
		"00/00000000000000000000000000000000000005",
		"4b/ba61b5499a7a511eb515594f3293a8741516ad/in",
		"4b/ba61b5499a7a511eb515594f3293a8741516ad/out",
	}
	for _, d := range dirs {
		if err := os.MkdirAll(path.Join(inputPath, d), 0755); err != nil {
			t.Fatalf("Failed to create %q: %q", d, err)
		}
	}
	files := []struct {
		filename, contents string
	}{
		{
			"00/00000000000000000000000000000000000000/settings.json",
			"{}",
		},
		{
			"00/00000000000000000000000000000000000000.sha1",
			"0000000000000000000000000000000000000000 *00000000000000000000000000000000000000/settings.json",
		},
		{
			"00/00000000000000000000000000000000000001/settings.json",
			"{}",
		},
		{
			"00/00000000000000000000000000000000000002.sha1",
			"0000000000000000000000000000000000000000 *settings.json",
		},
		{
			"00/00000000000000000000000000000000000003.sha1",
			"invalid sha1 file",
		},
		{
			"00/00000000000000000000000000000000000004.sha1",
			"",
		},
		{
			"00/00000000000000000000000000000000000005.sha1",
			"",
		},
		{
			"00/00000000000000000000000000000000000005/settings.json",
			"invalid json",
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad/in/0.in",
			"1 2",
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad/out/0.out",
			"3",
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad/settings.json",
			`{
  "Cases": [
		{"Cases": [{"Name": "0", "Weight": 1.0}], "Name": "0", "Weight": 1.0}
  ], 
  "Limits": {
    "ExtraWallTime": 0, 
    "MemoryLimit": 67108864, 
    "OutputLimit": 16384, 
    "OverallWallTimeLimit": 60000, 
    "TimeLimit": 3000, 
    "ValidatorTimeLimit": 3000
  }, 
  "Slow": false, 
	"Validator": {"Name": "token-numeric"}
}`,
		},
		{
			"4b/ba61b5499a7a511eb515594f3293a8741516ad.sha1",
			`0849b07a45865a148f85f3ef389d6b6fa8e2d1fb *ba61b5499a7a511eb515594f3293a8741516ad/settings.json
			3c28d037e32cd30eefd8183a83153083cced6cb7 *ba61b5499a7a511eb515594f3293a8741516ad/in/0.in
			77de68daecd823babbb58edb1c8e14d7106e83bb *ba61b5499a7a511eb515594f3293a8741516ad/out/0.out`,
		},
	}
	for _, ft := range files {
		if err := ioutil.WriteFile(
			path.Join(inputPath, ft.filename),
			[]byte(ft.contents),
			0644,
		); err != nil {
			t.Fatalf("Failed to write file: %q", err)
		}
	}
	var AplusBHash string
	{
		AplusB, err := common.NewLiteralInputFactory(
			&common.LiteralInput{
				Cases: map[string]*common.LiteralCaseSettings{
					"0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
					"1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(1, 1)},
				},
				Validator: &common.LiteralValidatorSettings{
					Name: "token-numeric",
				},
			},
			ctx.Config.Runner.RuntimePath,
			common.LiteralPersistRunner,
		)
		if err != nil {
			t.Fatalf("Failed to create InputFactory: %q", err)
		}
		inputManager := common.NewInputManager(ctx)
		AplusBInputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
		if err != nil {
			t.Fatalf("Failed to create Input: %q", err)
		}
		AplusBHash = AplusBInputRef.Input.Hash()
		if err = AplusBInputRef.Input.Persist(); err != nil {
			t.Fatalf("Failed to persist Input: %q", err)
		}
		AplusBInputRef.Release()
	}
	inputManager.PreloadInputs(
		inputPath,
		NewCachedInputFactory(inputPath),
		&sync.Mutex{},
	)

	hashentries := []struct {
		hash  string
		valid bool
	}{
		{"0000000000000000000000000000000000000000", false},
		{"0000000000000000000000000000000000000001", false},
		{"0000000000000000000000000000000000000002", false},
		{"0000000000000000000000000000000000000003", false},
		{"0000000000000000000000000000000000000004", false},
		{"4bba61b5499a7a511eb515594f3293a8741516ad", true},
		{AplusBHash, true},
	}
	for _, het := range hashentries {
		inputRef, err := inputManager.Add(het.hash, &common.CacheOnlyInputFactoryForTesting{})
		if het.valid {
			if err != nil {
				t.Errorf("InputManager.Add(%q, &common.CacheOnlyInputFactoryForTesting{}) == %q, want nil", het.hash, err)
			}
		} else {
			if err == nil {
				t.Errorf("InputManager.Add(%q, &common.CacheOnlyInputFactoryForTesting{}) == %q, want !nil", het.hash, err)
			}
		}
		if inputRef != nil {
			inputRef.Release()
		}
	}
}

func TestInputFactory(t *testing.T) {
	ctx, err := newRunnerContext(t)
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Runner.RuntimePath)

	type httpentry struct {
		content []byte
		mime    string
		headers map[string]string
	}
	httpentries := map[string]httpentry{
		"/input/4bba61b5499a7a511eb515594f3293a8741516ad/": {
			mustDecode(
				`H4sIAFBMXVYAA+2WzU+DMBTAd93+CsJZWUspEK/Gmx+HLXowHppZZx0UA2VqFv53X1HYXDKXmDCi
				vt+B0r6Pvva1D5QeD7qGABHntqURJ5ttw9d3ShllA4d3HhlQFkbkjjNYinkutNmpt0/+S1F6TDyl
				O53DJjUMgm/yz7bzH0WQf9JpVJ/88/xTx+87BKRHstJ0/gH4Qf3nNMD6fwhs/okHzw7n2F//6Vb+
				fYb1/zCwvgNAeqWQxig9L7ynIuvqL3Df/Q/hZ5/6oR/5Aec+1ALKOOF4/w/BauQ47qkoZOGeOLej
				4XC17q3cS5FKeHOJe+S4N1LNHw10qUeqOxjYLQWnoGBdn6tUGevNTgT9s1eTixuRJFNVG5NaDQQX
				Ms3yt1odhsOIkjgOg0Z6VZrn0jRSGrJ4LVrKHPw1PlsP9mQ1OhMjZovWmgQxj8JWuGnFNoyuRaLu
				hcnybQUQVx+rmyTZCww+iKSQMDJc29gVtxtksoXUx7pMZa5mdrOmWQJB61m9Ax5ch2pUjfo+CgiC
				IAiCIAiCIAiCIAiCIAiC/BHeAU4V1PQAKAAA`,
			),
			"application/x-gzip",
			map[string]string{
				"Content-Sha1":                "a32f0f018441df71efc8d1c6a55391354a8d4897",
				"X-Content-Uncompressed-Size": "20",
			},
		},
		"/input/0000000000000000000000000000000000000001/": {
			[]byte("invalid .tar.gz"),
			"application/x-gzip",
			map[string]string{
				"Content-Sha1":                "da39a3ee5e6b4b0d3255bfef95601890afd80709",
				"X-Content-Uncompressed-Size": "20",
			},
		},
		"/input/0000000000000000000000000000000000000002/": {
			mustDecode(
				`H4sIAFBMXVYAA+2WzU+DMBTAd93+CsJZWUspEK/Gmx+HLXowHppZZx0UA2VqFv53X1HYXDKXmDCi
				vt+B0r6Pvva1D5QeD7qGABHntqURJ5ttw9d3ShllA4d3HhlQFkbkjjNYinkutNmpt0/+S1F6TDyl
				O53DJjUMgm/yz7bzH0WQf9JpVJ/88/xTx+87BKRHstJ0/gH4Qf3nNMD6fwhs/okHzw7n2F//6Vb+
				fYb1/zCwvgNAeqWQxig9L7ynIuvqL3Df/Q/hZ5/6oR/5Aec+1ALKOOF4/w/BauQ47qkoZOGeOLej
				4XC17q3cS5FKeHOJe+S4N1LNHw10qUeqOxjYLQWnoGBdn6tUGevNTgT9s1eTixuRJFNVG5NaDQQX
				Ms3yt1odhsOIkjgOg0Z6VZrn0jRSGrJ4LVrKHPw1PlsP9mQ1OhMjZovWmgQxj8JWuGnFNoyuRaLu
				hcnybQUQVx+rmyTZCww+iKSQMDJc29gVtxtksoXUx7pMZa5mdrOmWQJB61m9Ax5ch2pUjfo+CgiC
				IAiCIAiCIAiCIAiCIAiC/BHeAU4V1PQAKAAA`,
			),
			"application/x-gzip",
			map[string]string{
				"Content-Sha1":                "Invalid SHA1 hash",
				"X-Content-Uncompressed-Size": "20",
			},
		},
		"/input/0000000000000000000000000000000000000003/": {
			mustDecode(
				`H4sIAFBMXVYAA+2WzU+DMBTAd93+CsJZWUspEK/Gmx+HLXowHppZZx0UA2VqFv53X1HYXDKXmDCi
				vt+B0r6Pvva1D5QeD7qGABHntqURJ5ttw9d3ShllA4d3HhlQFkbkjjNYinkutNmpt0/+S1F6TDyl
				O53DJjUMgm/yz7bzH0WQf9JpVJ/88/xTx+87BKRHstJ0/gH4Qf3nNMD6fwhs/okHzw7n2F//6Vb+
				fYb1/zCwvgNAeqWQxig9L7ynIuvqL3Df/Q/hZ5/6oR/5Aec+1ALKOOF4/w/BauQ47qkoZOGeOLej
				4XC17q3cS5FKeHOJe+S4N1LNHw10qUeqOxjYLQWnoGBdn6tUGevNTgT9s1eTixuRJFNVG5NaDQQX
				Ms3yt1odhsOIkjgOg0Z6VZrn0jRSGrJ4LVrKHPw1PlsP9mQ1OhMjZovWmgQxj8JWuGnFNoyuRaLu
				hcnybQUQVx+rmyTZCww+iKSQMDJc29gVtxtksoXUx7pMZa5mdrOmWQJB61m9Ax5ch2pUjfo+CgiC
				IAiCIAiCIAiCIAiCIAiC/BHeAU4V1PQAKAAA`,
			),
			"application/x-gzip",
			map[string]string{
				"Content-Sha1":                "a32f0f018441df71efc8d1c6a55391354a8d4897",
				"X-Content-Uncompressed-Size": "Invalid length",
			},
		},
		"/input/0000000000000000000000000000000000000004/": {
			mustDecode("H4sIAJctXVYAA8vMK0vMyUxRKEksUjAYKMAFAMWXZ/uGAAAA"),
			"application/x-gzip",
			map[string]string{},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry, ok := httpentries[r.RequestURI]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for k, v := range entry.headers {
			w.Header().Add(k, v)
		}
		w.Write(entry.content)
	}))
	defer ts.Close()
	ctx.Config.Runner.GraderURL = ts.URL

	inputManager := common.NewInputManager(ctx)

	hashentries := []struct {
		hash  string
		valid bool
	}{
		{"0000000000000000000000000000000000000000", false},
		{"0000000000000000000000000000000000000001", false},
		{"0000000000000000000000000000000000000002", false},
		{"0000000000000000000000000000000000000003", false},
		{"0000000000000000000000000000000000000004", false},
		{"4bba61b5499a7a511eb515594f3293a8741516ad", true},
	}
	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	factory := NewInputFactory(http.DefaultClient, &ctx.Config, baseURL, "")
	for _, het := range hashentries {
		inputRef, err := inputManager.Add(het.hash, factory)
		if het.valid {
			if err != nil {
				t.Errorf("Input creation failed with %q", err)
			}
		} else {
			if err == nil {
				t.Errorf("Input creation succeeded, but was expected to fail")
			}
		}
		if inputRef != nil {
			inputRef.Release()
		}
	}
}
