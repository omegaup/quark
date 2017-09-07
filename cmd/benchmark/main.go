package main

import (
	"encoding/json"
	"flag"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/runner"
	"io/ioutil"
	"os"
)

var (
	runtimePath = flag.String("runtime-path", "", "Override the runtime path")
	verbose     = flag.Bool("verbose", false, "Verbose logging")
	ioLock      common.FairMutex
	omegajail   runner.OmegajailSandbox
)

func main() {
	flag.Parse()

	config := common.DefaultConfig()

	if *runtimePath != "" {
		config.Runner.PreserveFiles = true
	} else {
		var err error
		if *runtimePath, err = ioutil.TempDir("", "runner"); err != nil {
			panic(err)
		}
		defer os.RemoveAll(*runtimePath)
	}
	config.Logging.File = "stderr"
	if *verbose {
		config.Logging.Level = "debug"
	}
	config.Runner.RuntimePath = *runtimePath
	config.Tracing.Enabled = false

	ctx, err := common.NewContext(&config)
	if err != nil {
		panic(err)
	}
	inputManager := common.NewInputManager(ctx)
	results, err := runner.RunHostBenchmark(
		ctx,
		inputManager,
		&omegajail,
		&ioLock,
	)
	if err != nil {
		panic(err)
	}
	encoded, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}

	os.Stdout.Write(encoded)
}
