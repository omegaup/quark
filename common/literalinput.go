package common

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
)

// LiteralCaseSettings stores the input, expected output, and the weight of a
// particular test case.
type LiteralCaseSettings struct {
	Input          string   `json:"in"`
	ExpectedOutput string   `json:"out"`
	Weight         *float64 `json:"weight,omitempty"`
}

// LiteralCustomValidatorSettings stores the source of the program that will
// validate the contestant's outputs.
type LiteralCustomValidatorSettings struct {
	Source   string          `json:"source"`
	Language string          `json:"language"`
	Limits   *LimitsSettings `json:"limits,omitempty"`
}

// LiteralValidatorSettings stores the settings for the validator, that will
// calculate a per-case grade. Valid values for Name are "custom", "literal",
// "token", "token-caseless", "token-numeric". If "custom" is chosen, a valid
// CustomValidator must be provided. If "token-numeric" is chosen, Tolerance
// must contain a numeric tolerance (typically a small number).
type LiteralValidatorSettings struct {
	Name            string                          `json:"name"`
	Tolerance       *float64                        `json:"tolerance,omitempty"`
	CustomValidator *LiteralCustomValidatorSettings `json:"custom_validator,omitempty"`
}

// LiteralInteractiveSettings stores the settings for a problem that uses
// libinteractive.
type LiteralInteractiveSettings struct {
	IDLSource  string `json:"idl"`
	ModuleName string `json:"module_name"`
	ParentLang string `json:"language"`
	MainSource string `json:"main_source"`
}

// Default values for some of the settings.
var (
	DefaultLiteralLimitSettings = LimitsSettings{
		TimeLimit:            1000,     // 1s
		MemoryLimit:          67108864, // 64MB
		OverallWallTimeLimit: 5000,     // 5s
		ExtraWallTime:        0,        // 0s
		OutputLimit:          10240,    // 10k
	}

	DefaultLiteralValidatorSettings = LiteralValidatorSettings{
		Name: "token-caseless",
	}

	DefaultValidatorTolerance = 1e-6
)

// LiteralInput is a standalone representation of an Input (although it cannot
// be used directly as an Input). It is useful for testing and to evaluate a
// run that doesn't have a problem associated with it.
type LiteralInput struct {
	Cases       map[string]LiteralCaseSettings `json:"cases"`
	Limits      *LimitsSettings                `json:"limits,omitempty"`
	Validator   *LiteralValidatorSettings      `json:"validator,omitempty"`
	Interactive *LiteralInteractiveSettings    `json:"interactive,omitempty"`
}

// LiteralRun is a standalone representation of a Run. It is useful for testing
// and to evaluate a run that doesn't have a problem associated with it.
type LiteralRun struct {
	Source   string `json:"source"`
	Language string `json:"language"`
	Input    string `json:"input"`
}

func validateLanguage(lang string) error {
	switch lang {
	case "c", "cpp", "cpp11", "kj", "kp", "java", "py", "pas", "rb", "cat":
		return nil
	default:
		return errors.New(fmt.Sprintf("invalid language %q", lang))
	}
}

func validateInterface(interfaceName string) error {
	if len(interfaceName) == 0 {
		return errors.New("empty interface name")
	}
	if len(interfaceName) > 32 {
		return errors.New(fmt.Sprintf(
			"interface name longer than 32 characters: %q",
			interfaceName,
		))
	}
	matched, err := regexp.MatchString("^[a-zA-Z_][0-9a-zA-Z]+$", interfaceName)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New(fmt.Sprintf("invalid interface name: %q", interfaceName))
	}
	return nil
}

// LiteralInputFactory is an InputFactory that will return an Input version of
// the specified LiteralInput when asked for an input.
type LiteralInputFactory struct {
	settings    ProblemSettings
	runtimePath string
	files       map[string][]byte
	hash        string
	tarfile     bytes.Buffer
}

// NewLiteralInputFactory validates the LiteralInput and stores it so it can be
// returned when NewInput is called.
func NewLiteralInputFactory(
	input *LiteralInput,
	runtimePath string,
) (*LiteralInputFactory, error) {
	factory := &LiteralInputFactory{
		runtimePath: runtimePath,
		settings: ProblemSettings{
			Slow: true,
		},
		files: make(map[string][]byte),
	}

	// Validator
	validator := input.Validator
	if validator == nil {
		validator = &DefaultLiteralValidatorSettings
	}
	switch validator.Name {
	case "custom":
		if validator.CustomValidator == nil {
			return nil, errors.New("custom validator empty")
		}
		factory.settings.Validator.Name = validator.Name
		factory.settings.Validator.Lang = &validator.CustomValidator.Language
		validatorFilename := fmt.Sprintf(
			"validator.%s",
			validator.CustomValidator.Language,
		)
		factory.files[validatorFilename] = []byte(validator.CustomValidator.Source)
		if validator.CustomValidator.Limits == nil {
			limits := DefaultValidatorLimits
			validator.CustomValidator.Limits = &limits
		}
	case "token", "token-caseless":
		factory.settings.Validator.Name = validator.Name
	case "token-numeric":
		factory.settings.Validator.Name = validator.Name
		if validator.Tolerance != nil {
			factory.settings.Validator.Tolerance = validator.Tolerance
		} else {
			factory.settings.Validator.Tolerance = &DefaultValidatorTolerance
		}
	default:
		return nil, errors.New(
			fmt.Sprintf("invalid validator %q", validator.Name),
		)
	}

	// Limits
	if input.Limits != nil {
		factory.settings.Limits.TimeLimit = min(
			input.Limits.TimeLimit,
			DefaultLiteralLimitSettings.TimeLimit,
		)
		factory.settings.Limits.MemoryLimit = min(
			input.Limits.MemoryLimit,
			DefaultLiteralLimitSettings.MemoryLimit,
		)
		factory.settings.Limits.OverallWallTimeLimit = min(
			input.Limits.OverallWallTimeLimit,
			DefaultLiteralLimitSettings.OverallWallTimeLimit,
		)
		factory.settings.Limits.ExtraWallTime = min(
			input.Limits.ExtraWallTime,
			DefaultLiteralLimitSettings.ExtraWallTime,
		)
		factory.settings.Limits.OutputLimit = min(
			input.Limits.OutputLimit,
			DefaultLiteralLimitSettings.OutputLimit,
		)
	} else {
		factory.settings.Limits = DefaultLiteralLimitSettings
	}

	// Cases
	if len(input.Cases) == 0 {
		return nil, errors.New("empty case list")
	}
	totalWeight := 0.0
	for _, c := range input.Cases {
		if c.Weight == nil {
			totalWeight += 1
		} else {
			if *c.Weight < 0 {
				return nil, errors.New(
					fmt.Sprintf("invalid weight, must be positive: %v", *c.Weight),
				)
			}
			totalWeight += *c.Weight
		}
	}
	if totalWeight == 0 {
		return nil, errors.New("weight must be positive")
	}
	groups := make(map[string][]CaseSettings)
	for name, c := range input.Cases {
		tokens := strings.SplitN(name, ".", 2)
		weight := 1.0
		if c.Weight != nil {
			weight = *c.Weight
		}
		cs := CaseSettings{
			Name:   name,
			Weight: weight / totalWeight,
		}
		if _, ok := groups[tokens[0]]; !ok {
			groups[tokens[0]] = make([]CaseSettings, 0)
		}
		groups[tokens[0]] = append(groups[tokens[0]], cs)
		factory.files[fmt.Sprintf("in/%s.in", name)] = []byte(c.Input)
		factory.files[fmt.Sprintf("out/%s.out", name)] = []byte(c.ExpectedOutput)
	}
	factory.settings.Cases = make([]GroupSettings, 0)
	for name, g := range groups {
		group := GroupSettings{
			Name:   name,
			Weight: 0,
			Cases:  g,
		}
		sort.Sort(ByCaseName(group.Cases))
		for _, c := range g {
			group.Weight += c.Weight
		}
		factory.settings.Cases = append(factory.settings.Cases, group)
	}
	sort.Sort(ByGroupName(factory.settings.Cases))

	// Interactive
	if input.Interactive != nil {
		interactive := input.Interactive
		if err := validateLanguage(interactive.ParentLang); err != nil {
			return nil, err
		}
		if err := validateInterface(interactive.ModuleName); err != nil {
			return nil, err
		}
		factory.files[fmt.Sprintf("interactive/Main.%s", interactive.ParentLang)] =
			[]byte(interactive.MainSource)
		factory.files[fmt.Sprintf("interactive/%s.idl", interactive.ModuleName)] =
			[]byte(interactive.IDLSource)
		cmd := exec.Command(
			"/usr/bin/java",
			"-jar",
			"/usr/share/java/libinteractive.jar",
			"json",
			"--module-name", interactive.ModuleName,
			"--parent-lang", interactive.ParentLang,
		)
		cmd.Stdin = strings.NewReader(interactive.IDLSource)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, err
		}
		if err = cmd.Start(); err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(stdout)
		if err := decoder.Decode(&factory.settings.Interactive); err != nil {
			var buf bytes.Buffer
			// Best-effort stderr reading.
			io.Copy(&buf, stderr)
			cmd.Wait()
			return nil, errors.New(string(buf.Bytes()))
		}
		if err := cmd.Wait(); err != nil {
			return nil, err
		}
	}

	bytes, err := json.MarshalIndent(factory.settings, "", "  ")
	if err != nil {
		return nil, err
	}
	factory.files["settings.json"] = bytes

	if err := createTar(&factory.tarfile, factory.files); err != nil {
		return nil, err
	}

	hash := sha1.New()
	if _, err := io.Copy(hash, &factory.tarfile); err != nil {
		return nil, err
	}

	factory.hash = fmt.Sprintf("%02x", hash.Sum(nil))

	return factory, nil
}

// NewInput returns the LiteralInput that was specified as the
// LiteralInputFactory's Input in its constructor.
func (factory *LiteralInputFactory) NewInput(hash string, mgr *InputManager) Input {
	if hash != factory.hash {
		return nil
	}
	return &inMemoryInput{
		BaseInput: *NewBaseInput(
			factory.hash,
			mgr,
		),
		archivePath: path.Join(
			factory.runtimePath,
			"cache",
			fmt.Sprintf("%s.tar.gz", hash),
		),
		path: path.Join(
			factory.runtimePath,
			"input",
			hash,
		),
		files:    &factory.files,
		tarfile:  &factory.tarfile,
		settings: &factory.settings,
	}
}

func (factory *LiteralInputFactory) Hash() string {
	return factory.hash
}

// inMemoryInput is an Input that is generated from a LiteralInput.
type inMemoryInput struct {
	BaseInput
	archivePath string
	path        string
	files       *map[string][]byte
	tarfile     *bytes.Buffer
	settings    *ProblemSettings
}

func (input *inMemoryInput) Path() string {
	return input.path
}

func (input *inMemoryInput) Settings() *ProblemSettings {
	return input.settings
}

func (input *inMemoryInput) Persist() error {
	// Write the Grader part of the Input.
	{
		if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
			return err
		}
		tarFile, err := os.Create(input.archivePath)
		if err != nil {
			return nil
		}
		defer tarFile.Close()
		if _, err := io.Copy(tarFile, input.tarfile); err != nil {
			return err
		}
		sha1sumFile, err := os.Create(fmt.Sprintf("%s.sha1", input.archivePath))
		if err != nil {
			return nil
		}
		defer sha1sumFile.Close()
		if _, err := fmt.Fprintf(
			sha1sumFile,
			"%s *%s.tar.gz\n",
			input.Hash(),
			input.Hash(),
		); err != nil {
			return err
		}
	}
	// Write the Runner part of the input.
	{
		if err := os.MkdirAll(path.Dir(input.path), 0755); err != nil {
			return err
		}
		sha1sumFile, err := os.Create(fmt.Sprintf("%s.sha1", input.path))
		if err != nil {
			return err
		}
		defer sha1sumFile.Close()
		for filename, contents := range *input.files {
			filePath := path.Join(input.path, filename)
			if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
				return err
			}
			f, err := os.Create(path.Join(filePath))
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Write(contents); err != nil {
				return err
			}
			hash := sha1.New()
			if _, err := hash.Write(contents); err != nil {
				return err
			}
			if _, err = fmt.Fprintf(
				sha1sumFile,
				"%0x *%s/%s\n",
				hash.Sum(nil),
				input.Hash(),
				filename,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (input *inMemoryInput) Delete() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
	os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
	os.Remove(input.archivePath)
	os.RemoveAll(fmt.Sprintf("%s.tmp", input.path))
	os.Remove(fmt.Sprintf("%s.sha1", input.path))
	return os.RemoveAll(input.path)
}

func createTar(buf *bytes.Buffer, files map[string][]byte) error {
	gz := gzip.NewWriter(buf)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	for name, contents := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(contents)),
		}
		if err := archive.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := archive.Write(contents); err != nil {
			return err
		}
	}

	return nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
