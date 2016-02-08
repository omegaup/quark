package runner

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

type expectedResult struct {
	output, runError string
	meta             *RunMetadata
}

type runnerTestCase struct {
	language, source       string
	expectedVerdict        string
	expectedScore          float64
	expectedCompileResults expectedResult
	expectedResults        map[string]expectedResult
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
	if _, err := ef.WriteString(
		sandbox.testCase.expectedCompileResults.runError,
	); err != nil {
		return nil, err
	}
	of, err := os.Create(outputFile)
	if err != nil {
		return nil, err
	}
	defer of.Close()
	if _, err := of.WriteString(
		sandbox.testCase.expectedCompileResults.output,
	); err != nil {
		return nil, err
	}
	return sandbox.testCase.expectedCompileResults.meta, nil
}

func (sandbox *fakeSandbox) Run(
	ctx *common.Context,
	input common.Input,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*RunMetadata, error) {
	caseName := strings.TrimSuffix(path.Base(outputFile), path.Ext(outputFile))
	results, ok := sandbox.testCase.expectedResults[caseName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("case %q not found", caseName))
	}
	ef, err := os.Create(errorFile)
	if err != nil {
		return nil, err
	}
	defer ef.Close()
	if _, err := ef.WriteString(results.runError); err != nil {
		return nil, err
	}
	of, err := os.Create(outputFile)
	if err != nil {
		return nil, err
	}
	defer of.Close()
	if _, err := of.WriteString(results.output); err != nil {
		return nil, err
	}
	return results.meta, nil
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
				"\"Tracing\": {\"Enabled\": false}, "+
				"\"InputManager\": {\"CacheSize\": 1024}, "+
				"\"Runner\": {\"RuntimePath\": %q},"+
				"\"Grader\": {\"RuntimePath\": %q}"+
				"}",
			dirname,
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

	inputManager := common.NewInputManager(ctx)
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3"},
				"1": {Input: "2 3", ExpectedOutput: "5"},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
		},
		&ctx.Config,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	inputManager.Add(AplusB.Hash(), AplusB)

	input, err := inputManager.Get(AplusB.Hash())
	if err != nil {
		t.Fatalf("Failed to open problem: %q", err)
	}

	runtests := []runnerTestCase{
		{
			"py",
			"print sum(map(int, raw_input().strip().split()))",
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"print 3",
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"print 2",
			"WA",
			0.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"2", "", &RunMetadata{Verdict: "OK"}},
				"1": {"2", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"if",
			"CE",
			0.0,
			expectedResult{
				"",
				`  File "test.py", line 1
	    if
		     ^
				 SyntaxError: invalid syntax`,
				&RunMetadata{ExitStatus: 1, Verdict: "RTE"},
			},
			map[string]expectedResult{},
		},
		{
			"c",
			"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cpp",
			"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"rb",
			"puts 3",
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"hs",
			"main = putStrLn \"3\"",
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"pas",
			`program Main;
			begin
				writeln ('3');
			end.`,
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"java",
			`class Main {
				public static void main(String[] args) {
					System.out.println('3');
				}
			}`,
			"PA",
			0.5,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
	}
	for idx, rte := range runtests {
		results, err := Grade(
			ctx,
			&bytes.Buffer{},
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

func TestLibinteractive(t *testing.T) {
	minijail := &MinijailSandbox{}
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if !minijail.Supported() {
		t.Skip("minijail sandbox not supported")
	}
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Runner.RuntimePath)

	inputManager := common.NewInputManager(ctx)
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3"},
				"1": {Input: "2 3", ExpectedOutput: "5"},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
			Interactive: &common.LiteralInteractiveSettings{
				IDLSource: `
					interface Main {};
					interface AplusB {
						int sum(int a, int b);
					};
				`,
				MainSource: `
					#include "AplusB.h"
					#include <iostream>
					using namespace std;
					int main() {
						int A, B;
						cin >> A >> B;
						cout << sum(A, B) << endl;
					}
				`,
				ModuleName: "AplusB",
				ParentLang: "cpp",
			},
		},
		&ctx.Config,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}
	inputManager.Add(AplusB.Hash(), AplusB)

	input, err := inputManager.Get(AplusB.Hash())
	if err != nil {
		t.Fatalf("Failed to open problem: %q", err)
	}

	runtests := []runnerTestCase{
		{
			"cpp",
			`
				#include <iostream>
				using namespace std;
				int main() {
					int A, B;
					cin >> A >> B;
					cout << A + B << endl;
				}
			`,
			"CE",
			0.0,
			expectedResult{
				"",
				`  File "test.py", line 1
	    if
		     ^
				 SyntaxError: invalid syntax`,
				&RunMetadata{ExitStatus: 1, Verdict: "RTE"},
			},
			map[string]expectedResult{},
		},
		{
			"cpp",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return -1;
				}
			`,
			"WA",
			0.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"-1", "", &RunMetadata{Verdict: "OK"}},
				"1": {"-1", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cpp",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
			`,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
	}
	for idx, rte := range runtests {
		results, err := Grade(
			ctx,
			&bytes.Buffer{},
			&common.Run{
				AttemptID: uint64(idx),
				Language:  rte.language,
				InputHash: input.Hash(),
				Source:    rte.source,
				MaxScore:  1.0,
			},
			input,
			minijail,
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
