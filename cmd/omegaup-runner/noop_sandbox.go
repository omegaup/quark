package main

import (
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"os"
)

type noopSandbox struct{}

func (*noopSandbox) Supported() bool {
	return true
}

func (*noopSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*runner.RunMetadata, error) {
	ctx.Log.Info("Running with the no-op Sandbox")
	for _, filename := range []string{outputFile, errorFile, metaFile} {
		f, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return &runner.RunMetadata{Verdict: "OK"}, nil
}

func (*noopSandbox) Run(
	ctx *common.Context,
	limits *common.LimitsSettings,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*runner.RunMetadata, error) {
	for _, filename := range []string{outputFile, errorFile, metaFile} {
		f, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		f.Close()
	}
	return &runner.RunMetadata{Verdict: "OK"}, nil
}
