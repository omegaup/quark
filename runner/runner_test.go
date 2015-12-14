package runner

import (
	"bytes"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
)

type runnerTestCase struct {
	language, source                            string
	expectedVerdict                             string
	expectedScore                               float64
	expectedCompileOutput, expectedCompileError string
	expectedCompileMeta                         *RunMetadata
	expectedRunOutput, expectedRunError         string
	expectedRunMeta                             *RunMetadata
}

type sandboxWrapper interface {
	sandbox(testCase *runnerTestCase) Sandbox
}

type minijailSandboxWrapper struct {
	minijail *MinijailSandbox
}

func (wrapper *minijailSandboxWrapper) sandbox(testCase *runnerTestCase) Sandbox {
	return wrapper.minijail
}

type fakeSandboxWrapper struct {
}

type fakeSandbox struct {
	testCase *runnerTestCase
}

func (sandbox *fakeSandbox) Supported() bool {
	return true
}

func (sandbox *fakeSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	ef, err := os.Create(errorFile)
	if err != nil {
		return nil, err
	}
	defer ef.Close()
	if _, err := ef.WriteString(sandbox.testCase.expectedCompileError); err != nil {
		return nil, err
	}
	of, err := os.Create(outputFile)
	if err != nil {
		return nil, err
	}
	defer of.Close()
	if _, err := of.WriteString(sandbox.testCase.expectedCompileOutput); err != nil {
		return nil, err
	}
	return sandbox.testCase.expectedCompileMeta, nil
}

func (sandbox *fakeSandbox) Run(
	ctx *common.Context,
	input common.Input,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*RunMetadata, error) {
	ef, err := os.Create(errorFile)
	if err != nil {
		return nil, err
	}
	defer ef.Close()
	if _, err := ef.WriteString(sandbox.testCase.expectedRunError); err != nil {
		return nil, err
	}
	of, err := os.Create(outputFile)
	if err != nil {
		return nil, err
	}
	defer of.Close()
	if _, err := of.WriteString(sandbox.testCase.expectedRunOutput); err != nil {
		return nil, err
	}
	return sandbox.testCase.expectedRunMeta, nil
}

func (wrapper *fakeSandboxWrapper) sandbox(testCase *runnerTestCase) Sandbox {
	return &fakeSandbox{testCase: testCase}
}

func newRunnerContext() (*common.Context, error) {
	dirname, err := ioutil.TempDir("/tmp", "runnertest")
	if err != nil {
		return nil, err
	}
	loggingConfig := "\"Logging\": {\"File\": \"stderr\"}"
	if testing.Verbose() {
		loggingConfig = "\"Logging\": {\"File\": \"stderr\", \"Level\": \"debug\"}"
	}
	ctx, err := common.NewContext(bytes.NewBufferString(
		fmt.Sprintf(
			"{"+
				loggingConfig+", "+
				"\"InputManager\": {\"CacheSize\": 1024}, "+
				"\"Runner\": {\"RuntimePath\": %q}"+
				"}",
			dirname,
		),
	))
	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func TestGrade(t *testing.T) {
	runGraderTests(t, &fakeSandboxWrapper{})
}

func TestGradeMinijail(t *testing.T) {
	minijail := &MinijailSandbox{}
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if !minijail.Supported() {
		t.Skip("minijail sandbox not supported")
	}
	runGraderTests(t, &minijailSandboxWrapper{minijail: minijail})
}

func runGraderTests(t *testing.T, wrapper sandboxWrapper) {
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Runner.RuntimePath)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer ts.Close()
	ctx.Config.Runner.GraderURL = ts.URL

	inputManager := common.NewInputManager(ctx)
	inputPath := path.Join(ctx.Config.Runner.RuntimePath, "input")
	// Setting up files.
	dirs := []string{
		"4bba61b5499a7a511eb515594f3293a8741516ad/in",
		"4bba61b5499a7a511eb515594f3293a8741516ad/out",
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
			"4bba61b5499a7a511eb515594f3293a8741516ad/in/0.in",
			"1 2",
		},
		{
			"4bba61b5499a7a511eb515594f3293a8741516ad/out/0.out",
			"3",
		},
		{
			"4bba61b5499a7a511eb515594f3293a8741516ad/settings.json",
			`{
  "Cases": [
		{"Cases": [{"Name": "0", "Weight": 1.0}], "Name": "0", "Weight": 1.0}
  ], 
  "Limits": {
    "ExtraWallTime": 0, 
    "MemoryLimit": 67108864, 
    "OutputLimit": 16384, 
    "OverallWallTimeLimit": 60000, 
    "StackLimit": 10485760, 
    "TimeLimit": 3000, 
    "ValidatorTimeLimit": 3000
  }, 
  "Slow": false, 
	"Validator": {"Name": "token-numeric", "Tolerance": 0.001}
}`,
		},
		{
			"4bba61b5499a7a511eb515594f3293a8741516ad.sha1",
			`0780d1c3688b0fa44453888849948a264553ed85 *4bba61b5499a7a511eb515594f3293a8741516ad/settings.json
			3c28d037e32cd30eefd8183a83153083cced6cb7 *4bba61b5499a7a511eb515594f3293a8741516ad/in/0.in
			77de68daecd823babbb58edb1c8e14d7106e83bb *4bba61b5499a7a511eb515594f3293a8741516ad/out/0.out`,
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
	inputManager.PreloadInputs(
		inputPath,
		NewRunnerCachedInputFactory(inputPath),
		&sync.Mutex{},
	)

	input, err := inputManager.Get("4bba61b5499a7a511eb515594f3293a8741516ad")
	if err != nil {
		t.Fatalf("Failed to open problem: %q", err)
	}
	baseURL, err := url.Parse(ts.URL)
	if err != nil {
		panic(err)
	}

	runtests := []runnerTestCase{
		{
			"py",
			"print 3",
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"py",
			"print 2",
			"WA",
			0.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"2",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"py",
			"if",
			"CE",
			0.0,
			"",
			`  File "test.py", line 1
	    if
		     ^
				 SyntaxError: invalid syntax`,
			&RunMetadata{ExitStatus: 1, Verdict: "RTE"},
			"",
			"",
			nil,
		},
		{
			"c",
			"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"cpp",
			"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"rb",
			"puts 3",
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"hs",
			"main = putStrLn \"3\"",
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"pas",
			`program Main;
			begin
				writeln ('3');
			end.`,
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
		{
			"java",
			`class Main {
				public static void main(String[] args) {
					System.out.println('3');
				}
			}`,
			"AC",
			1.0,
			"",
			"",
			&RunMetadata{Verdict: "OK"},
			"3",
			"",
			&RunMetadata{Verdict: "OK"},
		},
	}
	for idx, rte := range runtests {
		results, err := Grade(
			ctx,
			http.DefaultClient,
			baseURL,
			&common.Run{
				AttemptID: uint64(idx),
				Language:  rte.language,
				InputHash: input.Hash(),
				Source:    rte.source,
				MaxScore:  1.0,
			},
			input,
			wrapper.sandbox(&rte),
		)
		if err != nil {
			t.Errorf("Failed to run %v: %q", rte, err)
			continue
		}
		if results.Verdict != rte.expectedVerdict {
			t.Errorf(
				"results.Verdict = %q, expected %q",
				results.Verdict,
				rte.expectedVerdict,
			)
		}
		if results.Score != rte.expectedScore {
			t.Errorf(
				"results.Score = %v, expected %v",
				results.Score,
				rte.expectedScore,
			)
		}
	}
}

func TestWorseVerdict(t *testing.T) {
	verdictentries := []struct {
		a, b, expected string
	}{
		{"OK", "AC", "AC"},
		{"AC", "OK", "AC"},
		{"JE", "AC", "JE"},
	}
	for _, vet := range verdictentries {
		got := worseVerdict(vet.a, vet.b)
		if got != vet.expected {
			t.Errorf(
				"WorseVerdict(%q %q) == %q, expected %q",
				vet.a,
				vet.b,
				got,
				vet.expected,
			)
		}
	}
}
