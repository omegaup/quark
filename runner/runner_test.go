package runner

import (
	"bytes"
	"fmt"
	"github.com/omegaup/quark/common"
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
	maxScore               float64
	expectedVerdict        string
	expectedScore          float64
	expectedCompileResults expectedResult
	expectedResults        map[string]expectedResult
}

type sandboxWrapper interface {
	sandbox(testCase *runnerTestCase) Sandbox
}

type omegajailSandboxWrapper struct {
	omegajail *OmegajailSandbox
}

func (wrapper *omegajailSandboxWrapper) sandbox(testCase *runnerTestCase) Sandbox {
	return wrapper.omegajail
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
	limits *common.LimitsSettings,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*RunMetadata, error) {
	caseName := strings.TrimSuffix(path.Base(outputFile), path.Ext(outputFile))
	results, ok := sandbox.testCase.expectedResults[caseName]
	if !ok {
		return nil, fmt.Errorf("case %q not found", caseName)
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
	config := common.DefaultConfig()
	config.Logging.File = "stderr"
	if testing.Verbose() {
		config.Logging.Level = "debug"
	}
	config.Tracing.Enabled = false
	config.InputManager.CacheSize = 1024
	config.Runner.RuntimePath = dirname
	ctx, err := common.NewContext(&config, "runner")
	if err != nil {
		return nil, err
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

	return ctx, nil
}

func TestGrade(t *testing.T) {
	runGraderTests(t, &fakeSandboxWrapper{})
}

func TestGradeOmegajail(t *testing.T) {
	omegajail := &OmegajailSandbox{}
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if !omegajail.Supported() {
		t.Skip("omegajail sandbox not supported")
	}
	runGraderTests(t, &omegajailSandboxWrapper{omegajail: omegajail})
	runGraderTestsLowMem(t, &omegajailSandboxWrapper{omegajail: omegajail})
}

func runGraderTests(t *testing.T, wrapper sandboxWrapper) {
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Runner.RuntimePath)
	}

	inputManager := common.NewInputManager(ctx)
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0":   {Input: "1 2", ExpectedOutput: "3", Weight: &[]float64{1.0}[0]},
				"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: &[]float64{1.0}[0]},
				"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: &[]float64{2.0}[0]},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
		},
		ctx.Config.Runner.RuntimePath,
		common.LiteralPersistRunner,
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
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"ans = sum(map(int, raw_input().strip().split()))\n" +
				"assert ans <= 3\n" +
				"print ans",
			1.0,
			"RTE",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"", "", &RunMetadata{Verdict: "RTE"}},
			},
		},
		{
			"py",
			"print 3",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"print 2",
			1.0,
			"WA",
			0.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"2", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"2", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"2", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"py",
			"if",
			1.0,
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
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cpp",
			"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"rb",
			"puts 3",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"hs",
			"main = putStrLn \"3\"",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"pas",
			`program Main;
			begin
				writeln ('3');
			end.`,
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"lua",
			"a = io.read(\"*n\"); b = io.read(\"*n\"); io.write(a + b)",
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cs",
			`using System.Collections.Generic;
			using System.Linq;
			using System;

			class Program
			{
					static void Main(string[] args)
					{
							List<int> l = new List<int>();
							foreach (String token in Console.ReadLine().Trim().Split(' ')) {
								l.Add(Int32.Parse(token));
							}
							Console.WriteLine(l.Sum(x => x));
					}
			}`,
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cs",
			`using System.Collections.Generic;
			using System.Linq;
			using System;

			class Program
			{
					static void Main(string[] args)
					{
							List<int> l = new List<int>();
							foreach (String token in Console.ReadLine().Trim().Split(' ')) {
								for (int i = 0; i < 10000000; i++) {
									l.Add(Int32.Parse(token));
								}
							}
							Console.WriteLine(l.Sum(x => x));
					}
			}`,
			1.0,
			"MLE",
			0.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"", "", &RunMetadata{Verdict: "MLE"}},
				"1.0": {"", "", &RunMetadata{Verdict: "MLE"}},
				"1.1": {"", "", &RunMetadata{Verdict: "MLE"}},
			},
		},
		{
			"java",
			`import java.io.*;
			import java.util.*;
			class Main {
				public static void main(String[] args) throws IOException {
					long total = 0;
					try (Scanner in = new Scanner(new BufferedInputStream(System.in))) {
						while (in.hasNext()) {
							total += in.nextLong();
						}
					}
					System.out.println(total);
				}
			}`,
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"java",
			`import java.io.*;
			import java.util.*;
			class Main {
				public static void main(String[] args) throws IOException {
					long total = 0;
					try (Scanner in = new Scanner(new BufferedInputStream(System.in))) {
						while (in.hasNext()) {
							long x = in.nextLong();
							long[] arr = new long[1024 * 1024 * 160 * (int)x];
							for (int i = 0; i < arr.length; i++) {
								arr[i] = x;
							}
							long sum = 0;
							for (int i = arr.length - 1; i >= 0; i--) {
								sum += arr[i];
							}
							total += (sum % 2) + x;
						}
					}
					System.out.println(total);
				}
			}`,
			1.0,
			"MLE",
			0.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}},
				"1.0": {"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}},
				"1.1": {"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}},
			},
		},
		{
			"cat",
			"data:application/zip;base64,UEsDBAoAAAAAAOWiUUjRnmdVAgAAAAIAAAAFABwAMC5vdX" +
				"RVVAkAA67WxFb8t4ZYdXgLAAEE6AMAAAToAwAAMwpQSwMECgAAAAAAhhE4StGeZ1UCAAAAAg" +
				"AAAAcAHAAxLjAub3V0VVQJAAP8t4ZYCbiGWHV4CwABBOgDAAAE6AMAADMKUEsDBAoAAAAAAO" +
				"eiUUhXOT0DAgAAAAIAAAAHABwAMS4xLm91dFVUCQADstbEVgm4hlh1eAsAAQToAwAABOgDAA" +
				"A1ClBLAQIeAwoAAAAAAOWiUUjRnmdVAgAAAAIAAAAFABgAAAAAAAEAAAC0gQAAAAAwLm91dF" +
				"VUBQADrtbEVnV4CwABBOgDAAAE6AMAAFBLAQIeAwoAAAAAAIYROErRnmdVAgAAAAIAAAAHAB" +
				"gAAAAAAAEAAAC0gUEAAAAxLjAub3V0VVQFAAP8t4ZYdXgLAAEE6AMAAAToAwAAUEsBAh4DCg" +
				"AAAAAA56JRSFc5PQMCAAAAAgAAAAcAGAAAAAAAAQAAALSBhAAAADEuMS5vdXRVVAUAA7LWxF" +
				"Z1eAsAAQToAwAABOgDAABQSwUGAAAAAAMAAwDlAAAAxwAAAAAA",
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"java",
			`package foo;
			class Main {
				public static void main(String[] args) {
					System.out.println('3');
				}
			}`,
			1.0,
			"CE",
			0,
			expectedResult{
				"",
				"\nClass `Main` not found. Make sure your class is named `Main` and outside all packages",
				&RunMetadata{ExitStatus: 1, Verdict: "CE"},
			},
			map[string]expectedResult{},
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
				MaxScore:  rte.maxScore,
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
				"results.Verdict = %q, expected %q, test %v: %v",
				results.Verdict,
				rte.expectedVerdict,
				idx,
				rte,
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

func runGraderTestsLowMem(t *testing.T, wrapper sandboxWrapper) {
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Runner.RuntimePath)
	}

	inputManager := common.NewInputManager(ctx)
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0":   {Input: "1 2", ExpectedOutput: "3", Weight: &[]float64{1.0}[0]},
				"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: &[]float64{1.0}[0]},
				"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: &[]float64{2.0}[0]},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
			Limits: &common.LimitsSettings{
				TimeLimit:            common.DefaultLiteralLimitSettings.TimeLimit,
				MemoryLimit:          8 * 1024 * 1024,
				OverallWallTimeLimit: common.DefaultLiteralLimitSettings.OverallWallTimeLimit,
				ExtraWallTime:        common.DefaultLiteralLimitSettings.ExtraWallTime,
				OutputLimit:          common.DefaultLiteralLimitSettings.OutputLimit,
			},
		},
		ctx.Config.Runner.RuntimePath,
		common.LiteralPersistRunner,
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
			"c",
			"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"cpp",
			"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"pas",
			`program Main;
			begin
				writeln ('3');
			end.`,
			1.0,
			"PA",
			0.25,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0":   {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1.1": {"3", "", &RunMetadata{Verdict: "OK"}},
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
				MaxScore:  rte.maxScore,
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
				"results.Verdict = %q, expected %q, test %v: %v",
				results.Verdict,
				rte.expectedVerdict,
				idx,
				rte,
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

func TestKarelGrade(t *testing.T) {
	runKarelGraderTests(t, &fakeSandboxWrapper{})
}

func TestKarelGradeOmegajail(t *testing.T) {
	omegajail := &OmegajailSandbox{}
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if !omegajail.Supported() {
		t.Skip("omegajail sandbox not supported")
	}
	runKarelGraderTests(t, &omegajailSandboxWrapper{omegajail: omegajail})
}

func runKarelGraderTests(t *testing.T, wrapper sandboxWrapper) {
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Runner.RuntimePath)
	}

	inputManager := common.NewInputManager(ctx)
	expectedOutput := `<resultados>
	<mundos>
		<mundo nombre="mundo_0"/>
	</mundos>
	<programas>
		<programa nombre="p1" resultadoEjecucion="FIN PROGRAMA">
			<karel x="1" y="2"/>
		</programa>
	</programas>
</resultados>`
	AplusB, err := common.NewLiteralInputFactory(
		&common.LiteralInput{
			Cases: map[string]common.LiteralCaseSettings{
				"0": {Input: `<ejecucion>
	<condiciones instruccionesMaximasAEjecutar="10000000" longitudStack="65000"></condiciones>
	<mundos>
		<mundo nombre="mundo_0" ancho="100" alto="100">
			<monton x="1" y="2" zumbadores="1"></monton>
		</mundo>
	</mundos>
	<programas tipoEjecucion="CONTINUA" intruccionesCambioContexto="1" milisegundosParaPasoAutomatico="0">
		<programa nombre="p1" ruta="{$2$}" mundoDeEjecucion="mundo_0" xKarel="1" yKarel="1" direccionKarel="NORTE" mochilaKarel="0">
			<despliega tipo="MUNDO"></despliega>
			<despliega tipo="POSICION"></despliega>
		</programa>
	</programas>
</ejecucion>`, ExpectedOutput: expectedOutput, Weight: &[]float64{1.0}[0]},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: "token-numeric",
			},
		},
		ctx.Config.Runner.RuntimePath,
		common.LiteralPersistRunner,
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
			"kp",
			`
			iniciar-programa
				inicia-ejecucion
					mientras no-junto-a-zumbador hacer avanza;
					apagate;
				termina-ejecucion
			finalizar-programa
			`,
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {expectedOutput, "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"kj",
			"class program { program () { while (!nextToABeeper()) move(); turnoff(); } }",
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {expectedOutput, "", &RunMetadata{Verdict: "OK"}},
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
				MaxScore:  rte.maxScore,
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
				"results.Verdict = %q, expected %q, test %v: %v",
				results.Verdict,
				rte.expectedVerdict,
				idx,
				rte,
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
	omegajail := &OmegajailSandbox{}
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if !omegajail.Supported() {
		t.Skip("omegajail sandbox not supported")
	}
	ctx, err := newRunnerContext()
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(ctx.Config.Runner.RuntimePath)
	}

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
					interface Identity {
						int identity(int x);
					};
				`,
				MainSource: `
					#include "AplusB.h"
					#include <iostream>
					using namespace std;
					int main() {
						int A, B;
						cin >> A >> B;
						cout << identity(sum(identity(A), identity(B))) << endl;
					}
				`,
				ModuleName: "AplusB",
				ParentLang: "cpp",
			},
		},
		ctx.Config.Runner.RuntimePath,
		common.LiteralPersistRunner,
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
				int main(int argc, char* argv[]) {
					int A, B;
					cin >> A >> B;
					cerr << argv[1] << endl;
					cout << A + B << endl;
				}
			`,
			1.0,
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
			"cpp11",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return -1;
				}
				int identity(int x) {
					return -1;
				}
			`,
			1.0,
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
				int identity(int x) {
					return x;
				}
			`,
			1.0,
			"AC",
			1.0,
			expectedResult{"", "", &RunMetadata{Verdict: "OK"}},
			map[string]expectedResult{
				"0": {"3", "", &RunMetadata{Verdict: "OK"}},
				"1": {"5", "", &RunMetadata{Verdict: "OK"}},
			},
		},
		{
			"java",
			`
				class AplusB {
					public static int sum(int A, int B) {
						return A + B;
					}
				}
				class Identity {
					public static int identity(int x) {
						return x;
					}
				}
			`,
			1.0,
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
			"def sum(A, B):\n  return A + B\ndef identity(x):\n  return x",
			1.0,
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
				MaxScore:  rte.maxScore,
			},
			input,
			omegajail,
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
