package runner

import (
	"github.com/omegaup/quark/common"
	"math/big"
	"os"
)

type NoopSandbox struct{}

func (*NoopSandbox) Supported() bool {
	return true
}

func (*NoopSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	ctx.Log.Info("Running with the no-op Sandbox")
	for _, filename := range []string{outputFile, errorFile, metaFile} {
		f, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return &RunMetadata{Verdict: "OK"}, nil
}

func (*NoopSandbox) Run(
	ctx *common.Context,
	limits *common.LimitsSettings,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*RunMetadata, error) {
	for _, filename := range []string{outputFile, errorFile, metaFile} {
		f, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return &RunMetadata{Verdict: "OK"}, nil
}

func NoopSandboxFixupResult(result *RunResult) {
	// The no-op runner judges everything as AC.
	result.Verdict = "AC"
	result.Score = big.NewRat(1, 1)
	result.ContestScore = new(big.Rat).Mul(
		result.Score,
		result.MaxScore,
	)

	for i := range result.Groups {
		group := &result.Groups[i]
		group.Score = new(big.Rat).Add(
			&big.Rat{},
			group.MaxScore,
		)
		group.ContestScore = new(big.Rat).Mul(
			group.MaxScore,
			result.ContestScore,
		)

		for j := range group.Cases {
			caseResult := &group.Cases[j]
			caseResult.Score = new(big.Rat).Add(
				&big.Rat{},
				caseResult.MaxScore,
			)
			caseResult.ContestScore = new(big.Rat).Mul(
				caseResult.MaxScore,
				result.ContestScore,
			)
			caseResult.Verdict = "AC"

		}
	}
}
