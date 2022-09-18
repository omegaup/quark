package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/omegaup/go-base/logging/log15"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"
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
	log, err := log15.New("info", false)
	if err != nil {
		panic(err)
	}

	expected, err := os.Open(args[0])
	if err != nil {
		log.Error(
			"Unable to open expected file",
			map[string]any{
				"err": err,
			},
		)
		os.Exit(1)
	}
	defer expected.Close()

	contestant, err := os.Open(args[1])
	if err != nil {
		log.Error(
			"Unable to open contestant file",
			map[string]any{
				"err": err,
			},
		)
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
		log.Error(
			"Error validating",
			map[string]any{
				"err": err,
			},
		)
		os.Exit(1)
	}

	if mismatch != nil {
		log.Info(
			"Token mismatch",
			map[string]any{
				"expected": mismatch.Expected,
				"got":      mismatch.Contestant,
			},
		)
	}

	floatScore, _ := score.Float64()
	fmt.Printf("%.2f\n", floatScore)
}
