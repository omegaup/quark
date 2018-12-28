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
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LiteralPersistMode indicates whether the LiteralInputFactory should persist
// the input for the runner, grader, or neither.
type LiteralPersistMode int

const (
	// LiteralPersistNone indicates that the input should not be persisted.
	LiteralPersistNone LiteralPersistMode = iota

	// LiteralPersistGrader indicates that the input will be persisted with the
	// grader format.
	LiteralPersistGrader

	// LiteralPersistRunner indicates that the input will be persisted with the
	// runner format.
	LiteralPersistRunner
)

// LiteralCaseSettings stores the input, expected output, and the weight of a
// particular test case.
type LiteralCaseSettings struct {
	Input          string   `json:"in"`
	ExpectedOutput string   `json:"out"`
	Weight         *big.Rat `json:"weight"`
}

var _ json.Marshaler = &LiteralCaseSettings{}
var _ json.Unmarshaler = &LiteralCaseSettings{}

// MarshalJSON implements the json.Marshaler interface.
func (c *LiteralCaseSettings) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Input          string  `json:"in"`
		ExpectedOutput string  `json:"out"`
		Weight         float64 `json:"weight"`
	}{
		Input:          c.Input,
		ExpectedOutput: c.ExpectedOutput,
		Weight:         RationalToFloat(c.Weight),
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *LiteralCaseSettings) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	settings := struct {
		Input          string   `json:"in"`
		ExpectedOutput string   `json:"out"`
		Weight         *float64 `json:"weight"`
	}{}

	if err := json.Unmarshal(data, &settings); err != nil {
		return err
	}

	c.Input = settings.Input
	c.ExpectedOutput = settings.ExpectedOutput
	if settings.Weight == nil {
		c.Weight = big.NewRat(1, 1)
	} else {
		c.Weight = FloatToRational(*settings.Weight)
	}

	return nil
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
	IDLSource  string            `json:"idl"`
	ModuleName string            `json:"module_name"`
	ParentLang string            `json:"language"`
	MainSource string            `json:"main_source"`
	Templates  map[string]string `json:"templates"`
}

// Default values for some of the settings.
var (
	DefaultLiteralLimitSettings = LimitsSettings{
		TimeLimit:            Duration(time.Duration(10) * time.Second),
		MemoryLimit:          Gibibyte + Gibibyte/2,
		OverallWallTimeLimit: Duration(time.Duration(10) * time.Second),
		ExtraWallTime:        Duration(0),
		OutputLimit:          Byte(2) * Mebibyte,
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
	Cases       map[string]*LiteralCaseSettings `json:"cases"`
	Limits      *LimitsSettings                 `json:"limits,omitempty"`
	Validator   *LiteralValidatorSettings       `json:"validator,omitempty"`
	Interactive *LiteralInteractiveSettings     `json:"interactive,omitempty"`
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
		return fmt.Errorf("invalid language %q", lang)
	}
}

func validateInterface(interfaceName string) error {
	if len(interfaceName) == 0 {
		return errors.New("empty interface name")
	}
	if len(interfaceName) > 32 {
		return fmt.Errorf(
			"interface name longer than 32 characters: %q",
			interfaceName,
		)
	}
	matched, err := regexp.MatchString("^[a-zA-Z_][0-9a-zA-Z]+$", interfaceName)
	if err != nil {
		return err
	}
	if !matched {
		return fmt.Errorf("invalid interface name: %q", interfaceName)
	}
	return nil
}

// LanguageFileExtension returns the file extension for a particular language.
func LanguageFileExtension(language string) string {
	if language == "cpp11" {
		return "cpp"
	}
	return language
}

// LiteralInputFactory is an InputFactory that will return an Input version of
// the specified LiteralInput when asked for an input.
type LiteralInputFactory struct {
	settings         ProblemSettings
	persistMode      LiteralPersistMode
	runtimePath      string
	files            map[string][]byte
	hash             string
	tarfile          bytes.Buffer
	uncompressedSize int64
}

// NewLiteralInputFactory validates the LiteralInput and stores it so it can be
// returned when NewInput is called.
func NewLiteralInputFactory(
	input *LiteralInput,
	runtimePath string,
	persistMode LiteralPersistMode,
) (*LiteralInputFactory, error) {
	factory := &LiteralInputFactory{
		runtimePath: runtimePath,
		persistMode: persistMode,
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
	case "token", "token-caseless", "literal":
		factory.settings.Validator.Name = validator.Name
	case "token-numeric":
		factory.settings.Validator.Name = validator.Name
		if validator.Tolerance != nil {
			factory.settings.Validator.Tolerance = validator.Tolerance
		} else {
			factory.settings.Validator.Tolerance = &DefaultValidatorTolerance
		}
	default:
		return nil, fmt.Errorf("invalid validator %q", validator.Name)
	}

	// Limits
	if input.Limits != nil {
		factory.settings.Limits.TimeLimit = MinDuration(
			input.Limits.TimeLimit,
			DefaultLiteralLimitSettings.TimeLimit,
		)
		factory.settings.Limits.MemoryLimit = MinBytes(
			input.Limits.MemoryLimit,
			DefaultLiteralLimitSettings.MemoryLimit,
		)
		factory.settings.Limits.OverallWallTimeLimit = MinDuration(
			input.Limits.OverallWallTimeLimit,
			DefaultLiteralLimitSettings.OverallWallTimeLimit,
		)
		factory.settings.Limits.ExtraWallTime = MinDuration(
			input.Limits.ExtraWallTime,
			DefaultLiteralLimitSettings.ExtraWallTime,
		)
		factory.settings.Limits.OutputLimit = MinBytes(
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
	totalWeight := &big.Rat{}
	for _, c := range input.Cases {
		if c.Weight.Cmp(&big.Rat{}) < 0 {
			return nil, fmt.Errorf("invalid weight, must be positive: %v", *c.Weight)
		}
		totalWeight.Add(totalWeight, c.Weight)
	}
	if totalWeight.Cmp(&big.Rat{}) == 0 {
		return nil, errors.New("weight must be positive")
	}
	groups := make(map[string][]CaseSettings)
	for name, c := range input.Cases {
		tokens := strings.SplitN(name, ".", 2)
		weight := new(big.Rat).Add(&big.Rat{}, c.Weight)
		cs := CaseSettings{
			Name:   name,
			Weight: RationalDiv(weight, totalWeight),
		}
		if _, ok := groups[tokens[0]]; !ok {
			groups[tokens[0]] = make([]CaseSettings, 0)
		}
		groups[tokens[0]] = append(groups[tokens[0]], cs)
		factory.files[fmt.Sprintf("cases/%s.in", name)] = []byte(c.Input)
		factory.files[fmt.Sprintf("cases/%s.out", name)] = []byte(c.ExpectedOutput)
	}
	factory.settings.Cases = make([]GroupSettings, 0)
	for name, g := range groups {
		group := GroupSettings{
			Name:  name,
			Cases: g,
		}
		sort.Sort(ByCaseName(group.Cases))
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
		interactive.ParentLang = LanguageFileExtension(interactive.ParentLang)
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

	marshaledBytes, err := json.MarshalIndent(factory.settings, "", "  ")
	if err != nil {
		return nil, err
	}
	factory.files["settings.json"] = marshaledBytes

	for _, contents := range factory.files {
		factory.uncompressedSize += int64(len(contents))
	}

	if err := createTar(&factory.tarfile, factory.files); err != nil {
		return nil, err
	}

	hash := sha1.New()
	if _, err := io.Copy(hash, bytes.NewReader(factory.tarfile.Bytes())); err != nil {
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
			fmt.Sprintf("%s/%s.tar.gz", hash[:2], hash[2:]),
		),
		path: path.Join(
			factory.runtimePath,
			"input",
			fmt.Sprintf("%s/%s", hash[:2], hash[2:]),
		),
		files:            &factory.files,
		tarfile:          &factory.tarfile,
		settings:         &factory.settings,
		persistMode:      factory.persistMode,
		uncompressedSize: factory.uncompressedSize,
	}
}

// Hash returns the hash of the literal input.
func (factory *LiteralInputFactory) Hash() string {
	return factory.hash
}

// inMemoryInput is an Input that is generated from a LiteralInput.
type inMemoryInput struct {
	BaseInput
	archivePath      string
	path             string
	files            *map[string][]byte
	tarfile          *bytes.Buffer
	settings         *ProblemSettings
	persistMode      LiteralPersistMode
	uncompressedSize int64
}

func (input *inMemoryInput) Path() string {
	return input.path
}

func (input *inMemoryInput) Settings() *ProblemSettings {
	return input.settings
}

func (input *inMemoryInput) Size() int64 {
	return input.uncompressedSize
}

func (input *inMemoryInput) Persist() error {
	if input.persistMode == LiteralPersistGrader {
		if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
			return err
		}
		if err := ioutil.WriteFile(input.archivePath, input.tarfile.Bytes(), 0644); err != nil {
			return nil
		}
		lengthContents := []byte(strconv.FormatInt(input.uncompressedSize, 10))
		if err := ioutil.WriteFile(fmt.Sprintf("%s.len", input.archivePath), lengthContents, 0644); err != nil {
			return nil
		}
		hashContents := []byte(fmt.Sprintf(
			"%s *%s.tar.gz\n",
			input.Hash(),
			path.Base(input.archivePath),
		))
		if err := ioutil.WriteFile(fmt.Sprintf("%s.sha1", input.archivePath), hashContents, 0644); err != nil {
			return nil
		}
		input.Commit(int64(len(input.tarfile.Bytes())))
	}
	if input.persistMode == LiteralPersistRunner {
		if err := os.MkdirAll(path.Dir(input.path), 0755); err != nil {
			return err
		}
		sha1sumFile, err := os.Create(fmt.Sprintf("%s.sha1", input.path))
		if err != nil {
			return err
		}
		defer sha1sumFile.Close()
		var totalSize int64
		for filename, contents := range *input.files {
			filePath := path.Join(input.path, filename)
			if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
				return err
			}
			totalSize += int64(len(contents))
			if err := ioutil.WriteFile(filePath, contents, 0644); err != nil {
				return err
			}
			hash := sha1.New()
			if _, err := hash.Write(contents); err != nil {
				return err
			}
			written, err := fmt.Fprintf(
				sha1sumFile,
				"%0x *%s/%s\n",
				hash.Sum(nil),
				input.Hash()[2:],
				filename,
			)
			if err != nil {
				return err
			}
			totalSize += int64(written)
		}
		input.Commit(totalSize)
	}
	return nil
}

// Transmit sends a serialized version of the Input to the runner. It sends a
// .tar.gz file with the Content-SHA1 header with the hexadecimal
// representation of its SHA-1 hash.
func (input *inMemoryInput) Transmit(w http.ResponseWriter) error {
	fd, err := os.Open(input.archivePath)
	if err != nil {
		return err
	}
	defer fd.Close()
	w.Header().Add("Content-Type", "application/x-gzip")
	w.Header().Add("Content-SHA1", input.hash)
	w.Header().Add(
		"X-Content-Uncompressed-Size", strconv.FormatInt(input.uncompressedSize, 10),
	)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, fd)
	return err
}

func (input *inMemoryInput) Delete() error {
	if input.persistMode == LiteralPersistGrader {
		os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
		os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
		os.Remove(fmt.Sprintf("%s.len", input.archivePath))
		return os.Remove(input.archivePath)
	}
	if input.persistMode == LiteralPersistRunner {
		os.RemoveAll(fmt.Sprintf("%s.tmp", input.path))
		os.Remove(fmt.Sprintf("%s.sha1", input.path))
		return os.RemoveAll(input.path)
	}
	return nil
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
