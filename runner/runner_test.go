package runner

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"
)

type programOutput struct {
	stdout string
	stderr string
	meta   *RunMetadata
}

type expectedResult struct {
	runOutput       programOutput
	validatorOutput programOutput
}

type runnerTestCase struct {
	language, source       string
	maxScore               *big.Rat
	expectedVerdict        string
	expectedScore          *big.Rat
	expectedCompileResults expectedResult
	expectedResults        map[string]expectedResult
}

type sandboxWrapper interface {
	sandbox(testCase *runnerTestCase) Sandbox
	supported() bool
	name() string
}

type omegajailSandboxWrapper struct {
	omegajail *OmegajailSandbox
}

func (wrapper *omegajailSandboxWrapper) sandbox(testCase *runnerTestCase) Sandbox {
	return wrapper.omegajail
}

func (wrapper *omegajailSandboxWrapper) supported() bool {
	return wrapper.omegajail.Supported()
}

func (wrapper *omegajailSandboxWrapper) name() string {
	return "OmegajailSandbox"
}

type fakeSandboxWrapper struct {
}

type fakeSandbox struct {
	testCase *runnerTestCase
}

func (sandbox *fakeSandbox) Supported() bool {
	return true
}

func (sandbox *fakeSandbox) writeToFile(path, contents string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(
		contents,
	); err != nil {
		return err
	}
	return nil
}

func (sandbox *fakeSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	for _, ff := range []struct {
		path     string
		contents string
	}{
		{outputFile, sandbox.testCase.expectedCompileResults.runOutput.stdout},
		{errorFile, sandbox.testCase.expectedCompileResults.runOutput.stderr},
	} {
		if err := sandbox.writeToFile(ff.path, ff.contents); err != nil {
			return nil, err
		}
	}
	return sandbox.testCase.expectedCompileResults.runOutput.meta, nil
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
	var sandboxOutput *programOutput
	if strings.HasSuffix(inputFile, ".out") {
		// we're faking a validator
		sandboxOutput = &results.validatorOutput
	} else {
		sandboxOutput = &results.runOutput
	}
	for _, ff := range []struct {
		path     string
		contents string
	}{
		{outputFile, sandboxOutput.stdout},
		{errorFile, sandboxOutput.stderr},
	} {
		if err := sandbox.writeToFile(ff.path, ff.contents); err != nil {
			return nil, err
		}
	}
	return sandboxOutput.meta, nil
}

func (wrapper *fakeSandboxWrapper) sandbox(testCase *runnerTestCase) Sandbox {
	return &fakeSandbox{testCase: testCase}
}

func (wrapper *fakeSandboxWrapper) supported() bool {
	return true
}

func (wrapper *fakeSandboxWrapper) name() string {
	return "FakeSandbox"
}

func newRunnerContext(t *testing.T) (*common.Context, error) {
	dirname, err := ioutil.TempDir("/tmp", strings.ReplaceAll(t.Name(), "/", "_"))
	if err != nil {
		return nil, err
	}
	config := common.DefaultConfig()
	if testing.Verbose() {
		config.Logging.Level = "debug"
	}
	config.InputManager.CacheSize = 1024
	config.Runner.RuntimePath = dirname
	ctx, err := common.NewContext(&config)
	if err != nil {
		return nil, err
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

	return ctx, nil
}

func TestGrade(t *testing.T) {
	for name, wrapper := range map[string]sandboxWrapper{
		"fake":      &fakeSandboxWrapper{},
		"omegajail": &omegajailSandboxWrapper{omegajail: getSandbox()},
	} {
		wrapper := wrapper
		t.Run(name, func(t *testing.T) {
			if testing.Short() && wrapper.name() == "OmegajailSandbox" {
				t.Skip("skipping test in short mode.")
			}
			if !wrapper.supported() {
				t.Skip(fmt.Sprintf("%s not supported", wrapper.name()))
			}

			ctx, err := newRunnerContext(t)
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
					Cases: map[string]*common.LiteralCaseSettings{
						"0":   {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(2, 1)},
					},
					Validator: &common.LiteralValidatorSettings{
						Name: common.ValidatorNameTokenNumeric,
					},
					Limits: &common.LimitsSettings{
						TimeLimit:            base.Duration(time.Second),
						MemoryLimit:          64 * base.Mebibyte,
						OverallWallTimeLimit: base.Duration(time.Duration(5) * time.Second),
						ExtraWallTime:        base.Duration(0),
						OutputLimit:          10 * base.Kibibyte,
					},
				},
				ctx.Config.Runner.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				t.Fatalf("Failed to create Input: %q", err)
			}
			inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
			if err != nil {
				t.Fatalf("Failed to open problem: %q", err)
			}
			defer inputRef.Release()

			runtests := []runnerTestCase{
				{
					"py2",
					"print sum(map(int, raw_input().strip().split()))",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"py3",
					"print(sum(map(int, input().strip().split())))",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"py3",
					"ans = sum(map(int, input().strip().split()))\n" +
						"assert ans <= 3\n" +
						"print(ans)",
					big.NewRat(1, 1),
					"RTE",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"", "", &RunMetadata{Verdict: "RTE"}}},
					},
				},
				{
					"py3",
					"print(3)",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"py3",
					"print(2)",
					big.NewRat(1, 1),
					"WA",
					big.NewRat(0, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"2", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"2", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"2", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"py3",
					"if",
					big.NewRat(1, 1),
					"CE",
					big.NewRat(0, 1),
					expectedResult{
						runOutput: programOutput{
							"",
							`  File "test.py", line 1
	    if
		     ^
				 SyntaxError: invalid syntax`,
							&RunMetadata{ExitStatus: 1, Verdict: "RTE"}},
					},
					map[string]expectedResult{},
				},
				{
					"c11-gcc",
					"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"c11-clang",
					"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"cpp17-gcc",
					"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"cpp17-clang",
					"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"cpp20-gcc",
					"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"cpp20-clang",
					"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"rb",
					"puts 3",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"hs",
					"main = putStrLn \"3\"",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"pas",
					`program Main;
			begin
				writeln ('3');
			end.`,
					big.NewRat(1, 1),
					"PA",
					big.NewRat(1, 4),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"lua",
					"a = io.read(\"*n\"); b = io.read(\"*n\"); io.write(a + b)",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
					big.NewRat(1, 1),
					"MLE",
					big.NewRat(0, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"", "", &RunMetadata{Verdict: "MLE"}}},
						"1.0": {runOutput: programOutput{"", "", &RunMetadata{Verdict: "MLE"}}},
						"1.1": {runOutput: programOutput{"", "", &RunMetadata{Verdict: "MLE"}}},
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
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
					big.NewRat(1, 1),
					"MLE",
					big.NewRat(0, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}}},
						"1.0": {runOutput: programOutput{"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}}},
						"1.1": {runOutput: programOutput{"", "Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space", &RunMetadata{Verdict: "MLE"}}},
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
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
					big.NewRat(1, 1),
					"CE",
					big.NewRat(0, 1),
					expectedResult{
						runOutput: programOutput{
							"",
							"\nClass `Main` not found. Make sure your class is named `Main` and outside all packages",
							&RunMetadata{ExitStatus: 1, Verdict: "CE"},
						},
					},
					map[string]expectedResult{},
				},
				{
					"kt",
					`fun main() {
						val (a, b) = readLine()!!.split(' ').map(String::toInt)
					  println(a + b)
					}`,
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"go",
					`package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanWords)
	var sum int64
	for scanner.Scan() {
		v, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			panic(err)
		}
		sum += v
	}
	fmt.Println(sum)
}`,
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"rs",
					`use std::io;

fn main() -> io::Result<()> {
    let mut line = String::new();

    io::stdin().read_line(&mut line)?;
    let mut sum: i64 = 0;
    for s in line.trim().split(" ") {
        sum += i64::from_str_radix(s, 10).unwrap();
    }
    println!("{}", sum);

    Ok(())
}`,
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"js",
					`const readline = require("node:readline");

const rl = readline.createInterface({
  input: process.stdin,
});

rl.on("line", (input) => {
  console.log(
    input
      .trim()
      .split(" ")
      .map((x) => parseInt(x))
      .reduce((acc, x) => acc + x, 0)
  );
});`,
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
						"1.1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
					},
				},
			}
			for idx, rte := range runtests {
				t.Run(fmt.Sprintf("%s/%d/%s %s", wrapper.name(), idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
					results, err := Grade(
						ctx,
						&bytes.Buffer{},
						&common.Run{
							AttemptID: uint64(idx),
							Language:  rte.language,
							InputHash: inputRef.Input.Hash(),
							Source:    rte.source,
							MaxScore:  rte.maxScore,
						},
						inputRef.Input,
						wrapper.sandbox(&rte),
					)
					if err != nil {
						t.Fatalf("Failed to run %v: %q", rte, err)
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
					if results.Score.Cmp(rte.expectedScore) != 0 {
						t.Errorf(
							"results.Score = %s, expected %s",
							results.Score.String(),
							rte.expectedScore.String(),
						)
					}
				})
			}
		})
	}
}

func TestGradeGroupScorePolicySumIfNotZero(t *testing.T) {
	for name, wrapper := range map[string]sandboxWrapper{
		"fake":      &fakeSandboxWrapper{},
		"omegajail": &omegajailSandboxWrapper{omegajail: getSandbox()},
	} {
		wrapper := wrapper
		t.Run(name, func(t *testing.T) {
			if testing.Short() && wrapper.name() == "OmegajailSandbox" {
				t.Skip("skipping test in short mode.")
			}
			if !wrapper.supported() {
				t.Skip(fmt.Sprintf("%s not supported", wrapper.name()))
			}

			ctx, err := newRunnerContext(t)
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
					Cases: map[string]*common.LiteralCaseSettings{
						"0":   {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(2, 1)},
					},
					Validator: &common.LiteralValidatorSettings{
						Name:             common.ValidatorNameCustom,
						GroupScorePolicy: common.GroupScorePolicySumIfNotZero,
						CustomValidator: &common.LiteralCustomValidatorSettings{
							Source: `#!/usr/bin/python3

def _main() -> None:
  with open('data.in', 'r') as f:
    expected = float(f.read().strip())
  got = float(input().strip())
  print(f'{1 - abs(got - expected) / expected}')

if __name__ == '__main__':
  _main()
`,
							Language: "python3",
						},
					},
					Limits: &common.LimitsSettings{
						TimeLimit:            base.Duration(time.Second),
						MemoryLimit:          64 * base.Mebibyte,
						OverallWallTimeLimit: base.Duration(time.Duration(5) * time.Second),
						ExtraWallTime:        base.Duration(0),
						OutputLimit:          10 * base.Kibibyte,
					},
				},
				ctx.Config.Runner.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				t.Fatalf("Failed to create Input: %q", err)
			}
			inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
			if err != nil {
				t.Fatalf("Failed to open problem: %q", err)
			}
			defer inputRef.Release()

			runtests := []runnerTestCase{
				{
					"py3",
					"print(sum(map(int, input().strip().split())))",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"5", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
				{
					"py3",
					"print(3)",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(4, 5),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
				{
					"py3",
					"print(2)",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(8, 15),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6666666666666667", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6666666666666667", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.4", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
			}
			for idx, rte := range runtests {
				t.Run(fmt.Sprintf("%s/%d/%s %s", wrapper.name(), idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
					results, err := Grade(
						ctx,
						&bytes.Buffer{},
						&common.Run{
							AttemptID: uint64(idx),
							Language:  rte.language,
							InputHash: inputRef.Input.Hash(),
							Source:    rte.source,
							MaxScore:  rte.maxScore,
						},
						inputRef.Input,
						wrapper.sandbox(&rte),
					)
					if err != nil {
						t.Fatalf("Failed to run %v: %q", rte, err)
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
					if results.Score.Cmp(rte.expectedScore) != 0 {
						t.Errorf(
							"results.Score = %s, expected %s",
							results.Score.String(),
							rte.expectedScore.String(),
						)
					}
				})
			}
		})
	}
}

func TestGradeGroupScorePolicyMin(t *testing.T) {
	for name, wrapper := range map[string]sandboxWrapper{
		"fake":      &fakeSandboxWrapper{},
		"omegajail": &omegajailSandboxWrapper{omegajail: getSandbox()},
	} {
		wrapper := wrapper
		t.Run(name, func(t *testing.T) {
			if testing.Short() && wrapper.name() == "OmegajailSandbox" {
				t.Skip("skipping test in short mode.")
			}
			if !wrapper.supported() {
				t.Skip(fmt.Sprintf("%s not supported", wrapper.name()))
			}

			ctx, err := newRunnerContext(t)
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
					Cases: map[string]*common.LiteralCaseSettings{
						"0":   {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
						"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(2, 1)},
					},
					Validator: &common.LiteralValidatorSettings{
						Name:             common.ValidatorNameCustom,
						GroupScorePolicy: common.GroupScorePolicyMin,
						CustomValidator: &common.LiteralCustomValidatorSettings{
							Source: `#!/usr/bin/python3

def _main() -> None:
  with open('data.in', 'r') as f:
    expected = float(f.read().strip())
  got = float(input().strip())
  print(f'{1 - abs(got - expected) / expected}')

if __name__ == '__main__':
  _main()
`,
							Language: "python3",
						},
					},
					Limits: &common.LimitsSettings{
						TimeLimit:            base.Duration(time.Second),
						MemoryLimit:          64 * base.Mebibyte,
						OverallWallTimeLimit: base.Duration(time.Duration(5) * time.Second),
						ExtraWallTime:        base.Duration(0),
						OutputLimit:          10 * base.Kibibyte,
					},
				},
				ctx.Config.Runner.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				t.Fatalf("Failed to create Input: %q", err)
			}
			inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
			if err != nil {
				t.Fatalf("Failed to open problem: %q", err)
			}
			defer inputRef.Release()

			runtests := []runnerTestCase{
				{
					"py3",
					"print(sum(map(int, input().strip().split())))",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"5", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
				{
					"py3",
					"print(3)",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(7, 10),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"1", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
				{
					"py3",
					"print(2)",
					big.NewRat(1, 1),
					"PA",
					big.NewRat(7, 15),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6666666666666667", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.0": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.6666666666666667", "", &RunMetadata{Verdict: "OK"}},
						},
						"1.1": {
							runOutput:       programOutput{"2", "", &RunMetadata{Verdict: "OK"}},
							validatorOutput: programOutput{"0.4", "", &RunMetadata{Verdict: "OK"}},
						},
					},
				},
			}
			for idx, rte := range runtests {
				t.Run(fmt.Sprintf("%s/%d/%s %s", wrapper.name(), idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
					results, err := Grade(
						ctx,
						&bytes.Buffer{},
						&common.Run{
							AttemptID: uint64(idx),
							Language:  rte.language,
							InputHash: inputRef.Input.Hash(),
							Source:    rte.source,
							MaxScore:  rte.maxScore,
						},
						inputRef.Input,
						wrapper.sandbox(&rte),
					)
					if err != nil {
						t.Fatalf("Failed to run %v: %q", rte, err)
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
					if results.Score.Cmp(rte.expectedScore) != 0 {
						t.Errorf(
							"results.Score = %s, expected %s",
							results.Score.String(),
							rte.expectedScore.String(),
						)
					}
				})
			}
		})
	}
}

func TestGradeLowMemOmegajail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	omegajail := getSandbox()
	if !omegajail.Supported() {
		t.Skip("omegajail omegajail not supported")
	}

	ctx, err := newRunnerContext(t)
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
			Cases: map[string]*common.LiteralCaseSettings{
				"0":   {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
				"1.0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
				"1.1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(2, 1)},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: common.ValidatorNameTokenNumeric,
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

	inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
	if err != nil {
		t.Fatalf("Failed to open problem: %q", err)
	}
	defer inputRef.Release()

	runtests := []runnerTestCase{
		{
			"c11-gcc",
			"#include <stdio.h>\nint main() { printf(\"3\\n\"); }",
			big.NewRat(1, 1),
			"PA",
			big.NewRat(1, 4),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"cpp17-gcc",
			"#include <iostream>\nint main() { std::cout << \"3\\n\"; }",
			big.NewRat(1, 1),
			"PA",
			big.NewRat(1, 4),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"pas",
			`program Main;
			begin
				writeln ('3');
			end.`,
			big.NewRat(1, 1),
			"PA",
			big.NewRat(1, 4),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0":   {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1.1": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
	}
	for idx, rte := range runtests {
		t.Run(fmt.Sprintf("%d/%s %s", idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
			results, err := Grade(
				ctx,
				&bytes.Buffer{},
				&common.Run{
					AttemptID: uint64(idx),
					Language:  rte.language,
					InputHash: inputRef.Input.Hash(),
					Source:    rte.source,
					MaxScore:  rte.maxScore,
				},
				inputRef.Input,
				omegajail,
			)
			if err != nil {
				t.Fatalf("Failed to run %v: %q", rte, err)
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
			if results.Score.Cmp(rte.expectedScore) != 0 {
				t.Errorf(
					"results.Score = %s, expected %s",
					results.Score.String(),
					rte.expectedScore.String(),
				)
			}
		})
	}
}

func TestKarelGrade(t *testing.T) {
	for name, wrapper := range map[string]sandboxWrapper{
		"fake":      &fakeSandboxWrapper{},
		"omegajail": &omegajailSandboxWrapper{omegajail: getSandbox()},
	} {
		wrapper := wrapper
		t.Run(name, func(t *testing.T) {
			if testing.Short() && wrapper.name() == "OmegajailSandbox" {
				t.Skip("skipping test in short mode.")
			}
			if !wrapper.supported() {
				t.Skip(fmt.Sprintf("%s not supported", wrapper.name()))
			}

			ctx, err := newRunnerContext(t)
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
					Cases: map[string]*common.LiteralCaseSettings{
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
</ejecucion>`, ExpectedOutput: expectedOutput, Weight: big.NewRat(1, 1)},
					},
					Validator: &common.LiteralValidatorSettings{
						Name: common.ValidatorNameTokenNumeric,
					},
				},
				ctx.Config.Runner.RuntimePath,
				common.LiteralPersistRunner,
			)
			if err != nil {
				t.Fatalf("Failed to create Input: %q", err)
			}

			inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
			if err != nil {
				t.Fatalf("Failed to open problem: %q", err)
			}
			defer inputRef.Release()

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
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {runOutput: programOutput{expectedOutput, "", &RunMetadata{Verdict: "OK"}}},
					},
				},
				{
					"kj",
					"class program { program () { while (!nextToABeeper()) move(); turnoff(); } }",
					big.NewRat(1, 1),
					"AC",
					big.NewRat(1, 1),
					expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
					map[string]expectedResult{
						"0": {runOutput: programOutput{expectedOutput, "", &RunMetadata{Verdict: "OK"}}},
					},
				},
			}
			for idx, rte := range runtests {
				t.Run(fmt.Sprintf("%s/%d/%s %s", wrapper.name(), idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
					results, err := Grade(
						ctx,
						&bytes.Buffer{},
						&common.Run{
							AttemptID: uint64(idx),
							Language:  rte.language,
							InputHash: inputRef.Input.Hash(),
							Source:    rte.source,
							MaxScore:  rte.maxScore,
						},
						inputRef.Input,
						wrapper.sandbox(&rte),
					)
					if err != nil {
						t.Fatalf("Failed to run %v: %q", rte, err)
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
					if results.Score.Cmp(rte.expectedScore) != 0 {
						t.Errorf(
							"results.Score = %s, expected %s",
							results.Score.String(),
							rte.expectedScore.String(),
						)
					}
				})
			}
		})
	}
}

func TestLibinteractive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	omegajail := getSandbox()
	if !omegajail.Supported() {
		t.Skip("omegajail sandbox not supported")
	}
	ctx, err := newRunnerContext(t)
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
			Cases: map[string]*common.LiteralCaseSettings{
				"0": {Input: "1 2", ExpectedOutput: "3", Weight: big.NewRat(1, 1)},
				"1": {Input: "2 3", ExpectedOutput: "5", Weight: big.NewRat(1, 1)},
			},
			Validator: &common.LiteralValidatorSettings{
				Name: common.ValidatorNameTokenNumeric,
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
				Templates: map[string]string{
					"cpp": `
						#include "APlusB.h"

						int sum(int a, int b) {
							// FIXME
							return 0;
						}

						int identity(int x) {
							// FIXME
							return 0;
						}
					`,
				},
				ModuleName: "AplusB",
				ParentLang: "cpp17-gcc",
			},
		},
		ctx.Config.Runner.RuntimePath,
		common.LiteralPersistRunner,
	)
	if err != nil {
		t.Fatalf("Failed to create Input: %q", err)
	}

	inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
	if err != nil {
		t.Fatalf("Failed to open problem: %q", err)
	}
	defer inputRef.Release()

	runtests := []runnerTestCase{
		{
			"cpp17-gcc",
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
			big.NewRat(1, 1),
			"CE",
			big.NewRat(0, 1),
			expectedResult{
				runOutput: programOutput{
					"",
					`  File "test.py", line 1
			if
				^
					SyntaxError: invalid syntax`,
					&RunMetadata{ExitStatus: 1, Verdict: "RTE"},
				},
			},
			map[string]expectedResult{},
		},
		{
			"cpp17-clang",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return -1;
				}
				int identity(int x) {
					return -1;
				}
			`,
			big.NewRat(1, 1),
			"WA",
			big.NewRat(0, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"-1", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"-1", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"c",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"c11-gcc",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"c11-clang",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"cpp11",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"cpp17-gcc",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"cpp17-clang",
			`
				#include "AplusB.h"
				int sum(int A, int B) {
					return A + B;
				}
				int identity(int x) {
					return x;
				}
			`,
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
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
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"py",
			"def sum(A, B):\n  return A + B\ndef identity(x):\n  return x",
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"py2",
			"def sum(A, B):\n  return A + B\ndef identity(x):\n  return x",
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
		{
			"py3",
			"def sum(A, B):\n  return A + B\ndef identity(x):\n  return x",
			big.NewRat(1, 1),
			"AC",
			big.NewRat(1, 1),
			expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
			map[string]expectedResult{
				"0": {runOutput: programOutput{"3", "", &RunMetadata{Verdict: "OK"}}},
				"1": {runOutput: programOutput{"5", "", &RunMetadata{Verdict: "OK"}}},
			},
		},
	}
	for idx, rte := range runtests {
		t.Run(fmt.Sprintf("%d/%s %s", idx, rte.language, rte.expectedVerdict), func(t *testing.T) {
			results, err := Grade(
				ctx,
				&bytes.Buffer{},
				&common.Run{
					AttemptID: uint64(idx),
					Language:  rte.language,
					InputHash: inputRef.Input.Hash(),
					Source:    rte.source,
					MaxScore:  rte.maxScore,
				},
				inputRef.Input,
				omegajail,
			)
			if err != nil {
				t.Fatalf("Failed to run %v: %q", rte, err)
			}
			if results.Verdict != rte.expectedVerdict {
				t.Errorf(
					"results.Verdict = %q, expected %q",
					results.Verdict,
					rte.expectedVerdict,
				)
			}
			if results.Score.Cmp(rte.expectedScore) != 0 {
				t.Errorf(
					"results.Score = %s, expected %s",
					results.Score.String(),
					rte.expectedScore.String(),
				)
			}
		})
	}
}

func TestGradeWithCustomValidatorExpectedOutput(t *testing.T) {
	for wrapperName, wrapper := range map[string]sandboxWrapper{
		"fake":      &fakeSandboxWrapper{},
		"omegajail": &omegajailSandboxWrapper{omegajail: getSandbox()},
	} {
		wrapperName := wrapperName
		wrapper := wrapper
		t.Run(wrapperName, func(t *testing.T) {

			if testing.Short() && wrapper.name() == "OmegajailSandbox" {
				t.Skip("skipping test in short mode.")
			}

			if !wrapper.supported() {
				t.Skip(fmt.Sprintf("%s not supported", wrapper.name()))
			}

			for _, vv := range []struct {
				ValidatorName   string
				ValidatorSource string
				ExpectedOutput  programOutput
				ExpectedVerdict string
				ExpectedScore   *big.Rat
			}{
				{
					"expected AC",
					"import sys;print(1);print('wat',file=sys.stderr)",
					programOutput{"1", "wat", &RunMetadata{Verdict: "OK"}},
					"AC",
					big.NewRat(1, 1),
				},
				{
					"expected WA",
					"import sys;print(0);print('expected',file=sys.stderr)",
					programOutput{"0", "expected", &RunMetadata{Verdict: "OK"}},
					"WA",
					big.NewRat(0, 1),
				},
				{
					"unexpected WA",
					"import sys;print(0);print('something else',file=sys.stderr)",
					programOutput{"0", "something else", &RunMetadata{Verdict: "OK"}},
					"VE",
					big.NewRat(0, 1),
				},
				{
					"unexpected PA",
					"import sys;print(0);print('something else',file=sys.stderr)",
					programOutput{"0.5", "something else", &RunMetadata{Verdict: "OK"}},
					"VE",
					big.NewRat(0, 1),
				},
			} {
				validatorName := vv.ValidatorName
				validatorSource := vv.ValidatorSource
				expectedOutput := vv.ExpectedOutput
				expectedVerdict := vv.ExpectedVerdict
				expectedScore := vv.ExpectedScore

				t.Run(validatorName, func(t *testing.T) {

					ctx, err := newRunnerContext(t)
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
							Cases: map[string]*common.LiteralCaseSettings{
								"0":   {Input: "1 2", ExpectedOutput: "3", ExpectedValidatorStderr: "expected\n", Weight: big.NewRat(1, 1)},
								"1.0": {Input: "1 2", ExpectedOutput: "3", ExpectedValidatorStderr: "expected\n", Weight: big.NewRat(1, 1)},
								"1.1": {Input: "2 3", ExpectedOutput: "5", ExpectedValidatorStderr: "expected\n", Weight: big.NewRat(2, 1)},
							},
							Limits: &common.DefaultLimits,
							Validator: &common.LiteralValidatorSettings{
								Name: common.ValidatorNameCustom,
								CustomValidator: &common.LiteralCustomValidatorSettings{
									Language: "py3",
									Limits:   &common.DefaultValidatorLimits,
									Source:   validatorSource,
								},
							},
						},
						ctx.Config.Runner.RuntimePath,
						common.LiteralPersistRunner,
					)
					if err != nil {
						t.Fatalf("Failed to create Input: %q", err)
					}
					inputRef, err := inputManager.Add(AplusB.Hash(), AplusB)
					if err != nil {
						t.Fatalf("Failed to open problem: %q", err)
					}
					defer inputRef.Release()

					rte := runnerTestCase{
						"py2",
						"print sum(map(int, raw_input().strip().split()))",
						big.NewRat(1, 1),
						expectedVerdict,
						expectedScore,
						expectedResult{runOutput: programOutput{"", "", &RunMetadata{Verdict: "OK"}}},
						map[string]expectedResult{
							"0": {
								runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
								validatorOutput: expectedOutput,
							},
							"1.0": {
								runOutput:       programOutput{"3", "", &RunMetadata{Verdict: "OK"}},
								validatorOutput: expectedOutput,
							},
							"1.1": {
								runOutput:       programOutput{"5", "", &RunMetadata{Verdict: "OK"}},
								validatorOutput: expectedOutput,
							},
						},
					}
					results, err := Grade(
						ctx,
						&bytes.Buffer{},
						&common.Run{
							AttemptID: 0,
							Language:  rte.language,
							InputHash: inputRef.Input.Hash(),
							Source:    rte.source,
							MaxScore:  rte.maxScore,
						},
						inputRef.Input,
						wrapper.sandbox(&rte),
					)
					if err != nil {
						t.Fatalf("Failed to run %v: %q", rte, err)
					}
					if results.Verdict != rte.expectedVerdict {
						t.Errorf(
							"results.Verdict = %q, expected %q, test %v: %v",
							results.Verdict,
							rte.expectedVerdict,
							0,
							rte,
						)
					}
					if results.Score.Cmp(rte.expectedScore) != 0 {
						t.Errorf(
							"results.Score = %s, expected %s",
							results.Score.String(),
							rte.expectedScore.String(),
						)
					}
				})
			}
		})
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

func TestMergeVerdict(t *testing.T) {
	ctx, err := newRunnerContext(t)
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()

	noFaultVerdict := &RunMetadata{Verdict: "RTE", ExitStatus: 239}

	// Contestant has a no fault verdict.
	for _, entry := range []struct {
		b        *RunMetadata
		expected string
	}{
		{&RunMetadata{Verdict: "CE"}, "VE"},
		{&RunMetadata{Verdict: "RFE"}, "VE"},
		{&RunMetadata{Verdict: "MLE"}, "VE"},
		{&RunMetadata{Verdict: "RTE"}, "VE"},
		{&RunMetadata{Verdict: "TLE"}, "TLE"},
		{&RunMetadata{Verdict: "OLE"}, "OLE"},
		{&RunMetadata{Verdict: "OK"}, "RTE"},
	} {
		t.Run(fmt.Sprintf("mergeVerdict(noFault, %v)", entry.b.Verdict), func(t *testing.T) {
			got := mergeVerdict(ctx, noFaultVerdict, entry.b).Verdict
			if got != entry.expected {
				t.Errorf(
					"mergeVerdict().Verdict == %q, expected %q",
					got,
					entry.expected,
				)
			}
		})
	}

	// Contestant finished successfully.
	for _, entry := range []struct {
		b        *RunMetadata
		expected string
	}{
		{&RunMetadata{Verdict: "CE"}, "VE"},
		{&RunMetadata{Verdict: "RFE"}, "VE"},
		{&RunMetadata{Verdict: "MLE"}, "VE"},
		{&RunMetadata{Verdict: "RTE"}, "VE"},
		{&RunMetadata{Verdict: "TLE"}, "TLE"},
		{&RunMetadata{Verdict: "OLE"}, "OLE"},
		{&RunMetadata{Verdict: "OK"}, "OK"},
	} {
		t.Run(fmt.Sprintf("mergeVerdict(OK, %v)", entry.b.Verdict), func(t *testing.T) {
			got := mergeVerdict(ctx, &RunMetadata{Verdict: "OK"}, entry.b).Verdict
			if got != entry.expected {
				t.Errorf(
					"mergeVerdict().Verdict == %q, expected %q",
					got,
					entry.expected,
				)
			}
		})
	}

	// Parent has a no fault verdict.
	for _, entry := range []struct {
		a        *RunMetadata
		expected string
	}{
		{&RunMetadata{Verdict: "CE"}, "CE"},
		{&RunMetadata{Verdict: "RFE"}, "RFE"},
		{&RunMetadata{Verdict: "MLE"}, "MLE"},
		{&RunMetadata{Verdict: "RTE"}, "RTE"},
		{&RunMetadata{Verdict: "TLE"}, "TLE"},
		{&RunMetadata{Verdict: "OLE"}, "OLE"},
		{&RunMetadata{Verdict: "OK"}, "RTE"},
	} {
		t.Run(fmt.Sprintf("mergeVerdict(%v, noFault)", entry.a.Verdict), func(t *testing.T) {
			got := mergeVerdict(ctx, entry.a, noFaultVerdict).Verdict
			if got != entry.expected {
				t.Errorf(
					"mergeVerdict().Verdict == %q, expected %q",
					got,
					entry.expected,
				)
			}
		})
	}

	// Parent finished successfully.
	for _, entry := range []struct {
		a        *RunMetadata
		expected string
	}{
		{&RunMetadata{Verdict: "CE"}, "CE"},
		{&RunMetadata{Verdict: "RFE"}, "RFE"},
		{&RunMetadata{Verdict: "MLE"}, "MLE"},
		{&RunMetadata{Verdict: "RTE"}, "RTE"},
		{&RunMetadata{Verdict: "TLE"}, "TLE"},
		{&RunMetadata{Verdict: "OLE"}, "OLE"},
		{&RunMetadata{Verdict: "OK"}, "OK"},
	} {
		t.Run(fmt.Sprintf("mergeVerdict(%v, OK)", entry.a.Verdict), func(t *testing.T) {
			got := mergeVerdict(ctx, entry.a, &RunMetadata{Verdict: "OK"}).Verdict
			if got != entry.expected {
				t.Errorf(
					"mergeVerdict().Verdict == %q, expected %q",
					got,
					entry.expected,
				)
			}
		})
	}
}
