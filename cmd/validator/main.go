package main

import (
	"flag"
	"fmt"
	base "github.com/omegaup/go-base/v2"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
	"os"
)

var (
	validatorName = flag.String("validator", "", "name of validator")
	tolerance     = flag.Float64("tolerance", 1e-6, "tolerance (for numeric validators)")
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 || *validatorName == "" {
		flag.Usage()
		os.Exit(2)
	}
	log := base.StderrLog(false)

	expected, err := os.Open(args[0])
	if err != nil {
		log.Error("Unable to open expected file", "err", err)
		os.Exit(1)
	}
	defer expected.Close()

	contestant, err := os.Open(args[1])
	if err != nil {
		log.Error("Unable to open contestant file", "err", err)
		os.Exit(1)
	}
	defer contestant.Close()

	validator := &common.ValidatorSettings{
		Name:      common.ValidatorName(*validatorName),
		Tolerance: tolerance,
	}
	score, mismatch, err := runner.CalculateScore(
		validator,
		expected,
		contestant,
	)

	if err != nil {
		log.Error("Error validating", "err", err)
		os.Exit(1)
	}

	if mismatch != nil {
		log.Info(
			"Token mismatch",
			"expected", mismatch.Expected,
			"got", mismatch.Contestant,
		)
	}

	floatScore, _ := score.Float64()
	fmt.Printf("%.2f\n", floatScore)
}
