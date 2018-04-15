package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/grader/v1compat"
	"io"
	"os"
	"strconv"
	"time"
)

var (
	problemName = flag.String("problem-name", "", "Name of the problem")
	problemCsv  = flag.String("problem-csv", "", "Path of problems.csv")
	output      = flag.String("output", "",
		"Path where the .tar.gz file will be written")
	repositoryRoot = flag.String("repository-root", "",
		"Path where the .git repositories are located")
)

func mustParseBytes(s string) common.Byte {
	ret, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return common.Byte(ret)
}

func mustParseKibibytes(s string) common.Byte {
	ret, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return common.Byte(ret * 1024)
}

func mustParseMilliseconds(s string) common.Duration {
	ret, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return common.Duration(time.Duration(ret) * time.Millisecond)
}

func mustParseInt64(s string) int64 {
	ret, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return ret
}

func max64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type csvSettingsLoader struct {
	settings map[string]*common.ProblemSettings
}

func (loader *csvSettingsLoader) Load(
	problemName string,
) (*common.ProblemSettings, error) {
	settings, ok := loader.settings[problemName]
	if !ok {
		return nil, fmt.Errorf("unknown problem: %s", problemName)
	}
	settingsCopy := *settings
	return &settingsCopy, nil
}

func newCsvSettingsLoader(path string) (*csvSettingsLoader, error) {
	f, err := os.Open(*problemCsv)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	colMapping := make(map[string]int)

	for idx, name := range headers {
		colMapping[name] = idx
	}

	loader := &csvSettingsLoader{
		settings: make(map[string]*common.ProblemSettings),
	}

	rowIdx := 1
	for {
		row, err := csvReader.Read()
		rowIdx++
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}
		loader.settings[row[colMapping["alias"]]] = &common.ProblemSettings{
			Limits: common.LimitsSettings{
				ExtraWallTime:        mustParseMilliseconds(row[colMapping["extra_wall_time"]]),
				MemoryLimit:          mustParseKibibytes(row[colMapping["memory_limit"]]),
				OutputLimit:          mustParseBytes(row[colMapping["output_limit"]]),
				OverallWallTimeLimit: mustParseMilliseconds(row[colMapping["overall_wall_time_limit"]]),
				TimeLimit:            mustParseMilliseconds(row[colMapping["time_limit"]]),
			},
			Slow: mustParseInt64(row[colMapping["slow"]]) == 1,
			Validator: common.ValidatorSettings{
				Name: row[colMapping["validator"]],
				Limits: &common.LimitsSettings{
					ExtraWallTime: common.MaxDuration(
						common.DefaultValidatorLimits.ExtraWallTime,
						mustParseMilliseconds(row[colMapping["extra_wall_time"]]),
					),
					MemoryLimit: common.MaxBytes(
						common.DefaultValidatorLimits.MemoryLimit,
						mustParseKibibytes(row[colMapping["memory_limit"]]),
					),
					OutputLimit: common.MaxBytes(
						common.DefaultValidatorLimits.OutputLimit,
						mustParseBytes(row[colMapping["output_limit"]]),
					),
					OverallWallTimeLimit: common.MaxDuration(
						common.DefaultValidatorLimits.OverallWallTimeLimit,
						mustParseMilliseconds(row[colMapping["overall_wall_time_limit"]]),
					),
					TimeLimit: mustParseMilliseconds(row[colMapping["validator_time_limit"]]),
				},
			},
		}
	}

	return loader, nil
}

func main() {
	flag.Parse()

	if *problemName == "" {
		fmt.Fprintf(os.Stderr, "Missing -problem-name\n")
		return
	}
	if *problemCsv == "" {
		fmt.Fprintf(os.Stderr, "Missing -problem-csv\n")
		return
	}
	if *output == "" {
		fmt.Fprintf(os.Stderr, "Missing -output\n")
		return
	}
	if *repositoryRoot == "" {
		fmt.Fprintf(os.Stderr, "Missing -repository-root\n")
		return
	}

	repositoryPath := fmt.Sprintf("%s/%s", *repositoryRoot, *problemName)
	gitProblemInfo, err := v1compat.GetProblemInformation(repositoryPath)
	if err != nil {
		panic(err)
	}

	loader, err := newCsvSettingsLoader(*problemCsv)
	if err != nil {
		panic(err)
	}

	settings, err := loader.Load(*problemName)
	if err != nil {
		panic(err)
	}

	libinteractiveVersion, err := grader.GetLibinteractiveVersion()
	if err != nil {
		panic(err)
	}
	hash := v1compat.VersionedHash(libinteractiveVersion, gitProblemInfo, settings)

	_, _, err = v1compat.CreateArchiveFromGit(
		*problemName,
		*output,
		repositoryPath,
		hash,
		&v1compat.SettingsLoader{
			Settings: settings,
			GitTree:  gitProblemInfo.TreeID,
		},
	)
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(settings)
}
