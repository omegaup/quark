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

	base "github.com/omegaup/go-base/v3"
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
	Input                   string   `json:"in"`
	ExpectedOutput          string   `json:"out"`
	ExpectedValidatorStderr string   `json:"validator_stderr,omitempty"`
	Weight                  *big.Rat `json:"weight"`
}

var _ fmt.Stringer = &LiteralCaseSettings{}
var _ json.Marshaler = &LiteralCaseSettings{}
var _ json.Unmarshaler = &LiteralCaseSettings{}

// String implements the fmt.Stringer interface.
func (c *LiteralCaseSettings) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *c)
}

// MarshalJSON implements the json.Marshaler interface.
func (c *LiteralCaseSettings) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Input                   string  `json:"in"`
		ExpectedOutput          string  `json:"out"`
		ExpectedValidatorStderr string  `json:"validator_stderr,omitempty"`
		Weight                  float64 `json:"weight"`
	}{
		Input:                   c.Input,
		ExpectedOutput:          c.ExpectedOutput,
		ExpectedValidatorStderr: c.ExpectedValidatorStderr,
		Weight:                  base.RationalToFloat(c.Weight),
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *LiteralCaseSettings) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	settings := struct {
		Input                   string   `json:"in"`
		ExpectedOutput          string   `json:"out"`
		ExpectedValidatorStderr string   `json:"validator_stderr,omitempty"`
		Weight                  *float64 `json:"weight"`
	}{}

	if err := json.Unmarshal(data, &settings); err != nil {
		return err
	}

	c.Input = settings.Input
	c.ExpectedOutput = settings.ExpectedOutput
	c.ExpectedValidatorStderr = settings.ExpectedValidatorStderr
	if settings.Weight == nil {
		c.Weight = big.NewRat(1, 1)
	} else {
		c.Weight = base.FloatToRational(*settings.Weight)
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
	Name             ValidatorName                   `json:"name"`
	GroupScorePolicy GroupScorePolicy                `json:"group_score_policy,omitempty"`
	Tolerance        *float64                        `json:"tolerance,omitempty"`
	CustomValidator  *LiteralCustomValidatorSettings `json:"custom_validator,omitempty"`
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
		TimeLimit:            base.Duration(time.Duration(10) * time.Second),
		MemoryLimit:          base.Gibibyte + base.Gibibyte/2,
		OverallWallTimeLimit: base.Duration(time.Duration(10) * time.Second),
		ExtraWallTime:        base.Duration(0),
		OutputLimit:          base.Byte(20) * base.Mebibyte,
	}

	DefaultLiteralValidatorSettings = LiteralValidatorSettings{
		Name: ValidatorNameTokenCaseless,
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

// String implements the fmt.Stringer interface.
func (i *LiteralInput) String() string {
	if i == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *i)
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
	case "c", "c11-gcc", "c11-clang", "cpp", "cpp11", "cpp17-gcc", "cpp17-clang", "kj", "kp", "java", "py", "py2", "py3", "pas", "rb", "cat":
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
	switch language {
	case "cpp11", "cpp11-gcc", "cpp11-clang", "cpp17-gcc", "cpp17-clang", "cpp20-gcc", "cpp20-clang":
		return "cpp"
	case "c", "c11-gcc", "c11-clang":
		return "c"
	case "py", "py2", "py3":
		return "py"
	}
	return language
}

// FileExtensionLanguage returns the language for a particular file extension.
func FileExtensionLanguage(extension string) string {
	if extension == "cpp" {
		return "cpp11"
	}
	return extension
}

// LiteralInputFactory is an InputFactory that will return an Input version of
// the specified LiteralInput when asked for an input.
type LiteralInputFactory struct {
	input *inMemoryInput
	hash  string
}

// NewLiteralInputFactory validates the LiteralInput and stores it so it can be
// returned when NewInput is called.
func NewLiteralInputFactory(
	input *LiteralInput,
	runtimePath string,
	persistMode LiteralPersistMode,
) (*LiteralInputFactory, error) {
	settings := &ProblemSettings{
		Slow: true,
	}
	files := &map[string][]byte{}
	tarfile := &bytes.Buffer{}

	// Validator
	validator := input.Validator
	if validator == nil {
		validator = &DefaultLiteralValidatorSettings
	}
	settings.Validator.GroupScorePolicy = validator.GroupScorePolicy
	switch validator.Name {
	case ValidatorNameCustom:
		if validator.CustomValidator == nil {
			return nil, errors.New("custom validator empty")
		}
		settings.Validator.Name = validator.Name
		settings.Validator.Lang = &validator.CustomValidator.Language
		validatorFilename := fmt.Sprintf(
			"validator.%s",
			validator.CustomValidator.Language,
		)
		(*files)[validatorFilename] = []byte(validator.CustomValidator.Source)
		if validator.CustomValidator.Limits == nil {
			limits := DefaultValidatorLimits
			validator.CustomValidator.Limits = &limits
		}
	case ValidatorNameToken, ValidatorNameTokenCaseless, ValidatorNameLiteral:
		settings.Validator.Name = validator.Name
	case ValidatorNameTokenNumeric:
		settings.Validator.Name = validator.Name
		if validator.Tolerance != nil {
			settings.Validator.Tolerance = validator.Tolerance
		} else {
			settings.Validator.Tolerance = &DefaultValidatorTolerance
		}
	default:
		return nil, fmt.Errorf("invalid validator %q", validator.Name)
	}

	// Limits
	if input.Limits != nil {
		settings.Limits.TimeLimit = base.Min(
			input.Limits.TimeLimit,
			DefaultLiteralLimitSettings.TimeLimit,
		)
		settings.Limits.MemoryLimit = base.Min(
			input.Limits.MemoryLimit,
			DefaultLiteralLimitSettings.MemoryLimit,
		)
		settings.Limits.OverallWallTimeLimit = base.Min(
			input.Limits.OverallWallTimeLimit,
			DefaultLiteralLimitSettings.OverallWallTimeLimit,
		)
		settings.Limits.ExtraWallTime = base.Min(
			input.Limits.ExtraWallTime,
			DefaultLiteralLimitSettings.ExtraWallTime,
		)
		settings.Limits.OutputLimit = base.Min(
			input.Limits.OutputLimit,
			DefaultLiteralLimitSettings.OutputLimit,
		)
	} else {
		settings.Limits = DefaultLiteralLimitSettings
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
			Weight: base.RationalDiv(weight, totalWeight),
		}
		if _, ok := groups[tokens[0]]; !ok {
			groups[tokens[0]] = make([]CaseSettings, 0)
		}
		groups[tokens[0]] = append(groups[tokens[0]], cs)
		(*files)[fmt.Sprintf("cases/%s.in", name)] = []byte(c.Input)
		(*files)[fmt.Sprintf("cases/%s.out", name)] = []byte(c.ExpectedOutput)
		if c.ExpectedValidatorStderr != "" {
			(*files)[fmt.Sprintf("cases/%s.expected-failure", name)] = []byte(c.ExpectedValidatorStderr)
		}
	}
	settings.Cases = make([]GroupSettings, 0)
	for name, g := range groups {
		group := GroupSettings{
			Name:  name,
			Cases: g,
		}
		sort.Sort(ByCaseName(group.Cases))
		settings.Cases = append(settings.Cases, group)
	}
	sort.Sort(ByGroupName(settings.Cases))

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
		(*files)[fmt.Sprintf("interactive/Main.%s", interactive.ParentLang)] =
			[]byte(interactive.MainSource)
		(*files)[fmt.Sprintf("interactive/%s.idl", interactive.ModuleName)] =
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
		if err := decoder.Decode(&settings.Interactive); err != nil {
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

	marshaledBytes, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return nil, err
	}
	(*files)["settings.json"] = marshaledBytes

	var uncompressedSize int64
	for _, contents := range *files {
		uncompressedSize += int64(len(contents))
	}

	if err := createTar(tarfile, files); err != nil {
		return nil, err
	}

	hash := sha1.New()
	if _, err := io.Copy(hash, bytes.NewReader(tarfile.Bytes())); err != nil {
		return nil, err
	}

	inputHash := fmt.Sprintf("%02x", hash.Sum(nil))
	return &LiteralInputFactory{
		hash: inputHash,
		input: &inMemoryInput{
			archivePath: path.Join(
				runtimePath,
				"cache",
				fmt.Sprintf("%s/%s.tar.gz", inputHash[:2], inputHash[2:]),
			),
			path: path.Join(
				runtimePath,
				"input",
				fmt.Sprintf("%s/%s", inputHash[:2], inputHash[2:]),
			),
			files:            files,
			tarfile:          tarfile,
			settings:         settings,
			persistMode:      persistMode,
			uncompressedSize: base.Byte(uncompressedSize),
		},
	}, nil
}

// NewInput returns the LiteralInput that was specified as the
// LiteralInputFactory's Input in its constructor.
func (factory *LiteralInputFactory) NewInput(hash string, mgr *InputManager) Input {
	if hash != factory.hash || factory.input == nil {
		return nil
	}
	// Release the underlying input from the factory so that all the
	// memory-consuming elements of it are not unnecessarily held onto longer
	// than needed.
	passedInput := factory.input
	factory.input = nil
	passedInput.BaseInput = *NewBaseInput(hash, mgr)
	return passedInput
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
	uncompressedSize base.Byte
}

// Verify ensures that the disk representation of the Input is still valid.
// This returns ErrUnimplemented unconditionally so that it is always
// persisted.
func (input *inMemoryInput) Verify() error {
	return ErrUnimplemented
}

func (input *inMemoryInput) Path() string {
	return input.path
}

func (input *inMemoryInput) Settings() *ProblemSettings {
	return input.settings
}

func (input *inMemoryInput) Size() base.Byte {
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
		lengthContents := []byte(strconv.FormatInt(input.uncompressedSize.Bytes(), 10))
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

		// Now that the input has been persisted, release the memory-consuming elements.
		input.tarfile = nil
		input.files = nil
		input.settings = nil
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
		"X-Content-Uncompressed-Size", strconv.FormatInt(input.uncompressedSize.Bytes(), 10),
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

func (input *inMemoryInput) Release() {
	input.Delete()
}

func (input *inMemoryInput) Value() Input {
	return input
}

func createTar(buf *bytes.Buffer, files *map[string][]byte) error {
	gz := gzip.NewWriter(buf)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	var sortedFiles []string
	for name := range *files {
		sortedFiles = append(sortedFiles, name)
	}
	sort.Strings(sortedFiles)

	for _, name := range sortedFiles {
		contents := (*files)[name]
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
