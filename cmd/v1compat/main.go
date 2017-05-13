package main

import (
	"crypto/sha1"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/grader/v1compat"
	git "github.com/libgit2/git2go"
	"io"
	"os"
	"strconv"
)

var (
	problemName = flag.String("problem-name", "", "Name of the problem")
	problemCsv  = flag.String("problem-csv", "", "Path of problems.csv")
	output      = flag.String("output", "",
		"Path where the .tar.gz file will be written")
	repositoryRoot = flag.String("repository-root", "",
		"Path where the .git repositories are located")
)

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
		rowIdx += 1
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}
		loader.settings[row[colMapping["alias"]]] = &common.ProblemSettings{
			Limits: common.LimitsSettings{
				ExtraWallTime:        mustParseInt64(row[colMapping["extra_wall_time"]]),
				MemoryLimit:          mustParseInt64(row[colMapping["memory_limit"]]) * 1024,
				OutputLimit:          mustParseInt64(row[colMapping["output_limit"]]),
				OverallWallTimeLimit: mustParseInt64(row[colMapping["overall_wall_time_limit"]]),
				TimeLimit:            mustParseInt64(row[colMapping["time_limit"]]),
			},
			Slow: mustParseInt64(row[colMapping["slow"]]) == 1,
			Validator: common.ValidatorSettings{
				Name: row[colMapping["validator"]],
				Limits: &common.LimitsSettings{
					ExtraWallTime: max64(
						common.DefaultValidatorLimits.ExtraWallTime,
						mustParseInt64(row[colMapping["extra_wall_time"]]),
					),
					MemoryLimit: max64(
						common.DefaultValidatorLimits.MemoryLimit,
						mustParseInt64(row[colMapping["memory_limit"]])*1024,
					),
					OutputLimit: max64(
						common.DefaultValidatorLimits.OutputLimit,
						mustParseInt64(row[colMapping["output_limit"]]),
					),
					OverallWallTimeLimit: max64(
						common.DefaultValidatorLimits.OverallWallTimeLimit,
						mustParseInt64(row[colMapping["overall_wall_time_limit"]]),
					),
					TimeLimit: mustParseInt64(row[colMapping["validator_time_limit"]]),
				},
			},
		}
	}

	return loader, nil
}

func v1CompatGetTreeId(repositoryPath string) (string, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return "", err
	}
	defer repository.Free()
	headRef, err := repository.Head()
	if err != nil {
		return "", err
	}
	defer headRef.Free()
	headObject, err := headRef.Peel(git.ObjectCommit)
	if err != nil {
		return "", err
	}
	defer headObject.Free()
	headCommit, err := headObject.AsCommit()
	if err != nil {
		return "", err
	}
	defer headCommit.Free()
	return headCommit.TreeId().String(), nil
}

func versionedHash(
	hash string,
	settings *common.ProblemSettings,
) string {
	hasher := sha1.New()
	fmt.Fprint(hasher, "%d:", v1compat.InputVersion)
	io.WriteString(hasher, hash)
	io.WriteString(hasher, ":")
	json.NewEncoder(hasher).Encode(settings)
	return fmt.Sprintf("%0x", hasher.Sum(nil))
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
	gitTree, err := v1CompatGetTreeId(repositoryPath)
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
	hash := versionedHash(gitTree, settings)

	_, _, err = v1compat.CreateArchiveFromGit(
		*problemName,
		*output,
		repositoryPath,
		hash,
		&v1compat.SettingsLoader{
			Settings: settings,
			GitTree:  gitTree,
		},
	)
	if err != nil {
		panic(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(settings)
}
