package runner

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"

	"github.com/kballard/go-shellquote"
	"github.com/pkg/errors"
)

// Preloads an input so that the contestant's program has to wait less time.
type inputPreloader struct {
	file     *os.File
	fileSize int64
	mapping  []byte
	checksum uint8
}

func newInputPreloader(filePath string) (*inputPreloader, error) {
	if filePath == "/dev/null" {
		return nil, nil
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	preloader := &inputPreloader{
		file:     file,
		fileSize: info.Size(),
	}
	mapping, err := syscall.Mmap(
		int(preloader.file.Fd()),
		0,
		int(preloader.fileSize),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err == nil {
		pageSize := os.Getpagesize()
		preloader.mapping = mapping
		for i := 0; i < int(preloader.fileSize); i += pageSize {
			preloader.checksum += preloader.mapping[i]
		}
	} else {
		// mmap failed, so just read all the file.
		io.Copy(ioutil.Discard, preloader.file)
	}
	return preloader, nil
}

func (preloader *inputPreloader) release() {
	if preloader.mapping != nil {
		syscall.Munmap(preloader.mapping)
	}
	preloader.file.Close()
}

// RunMetadata represents the results of an execution.
type RunMetadata struct {
	Verdict    string    `json:"verdict"`
	ExitStatus int       `json:"exit_status,omitempty"`
	Time       float64   `json:"time"`
	SystemTime float64   `json:"sys_time"`
	WallTime   float64   `json:"wall_time"`
	Memory     base.Byte `json:"memory"`
	OutputSize base.Byte `json:"output_size"`
	Signal     *string   `json:"signal,omitempty"`
	Syscall    *string   `json:"syscall,omitempty"`
}

func (m *RunMetadata) String() string {
	metadata := fmt.Sprintf(
		"{Verdict: %s, ExitStatus: %d, Time: %.3fs, SystemTime: %.3fs, WallTime: %.3fs, Memory: %.3fMiB, OutputSize: %.3fMiB",
		m.Verdict,
		m.ExitStatus,
		m.Time,
		m.SystemTime,
		m.WallTime,
		m.Memory.Mebibytes(),
		m.OutputSize.Mebibytes(),
	)
	if m.Signal != nil {
		metadata += fmt.Sprintf(", Signal: %s", *m.Signal)
	}
	if m.Syscall != nil {
		metadata += fmt.Sprintf(", Syscall: %s", *m.Syscall)
	}
	metadata += "}"
	return metadata
}

// A Sandbox provides a mechanism to compile and run contestant-provided
// programs in a safe manner.
type Sandbox interface {
	// Supported returns true if the sandbox is available in the system.
	Supported() bool

	// Compile performs a compilation in the specified language.
	Compile(
		ctx *common.Context,
		lang string,
		inputFiles []string,
		chdir, outputFile, errorFile, metaFile, target string,
		extraFlags []string,
	) (*RunMetadata, error)

	// Run uses a previously compiled program and runs it against a single test
	// case with the supplied limits.
	Run(
		ctx *common.Context,
		limits *common.LimitsSettings,
		lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
		originalInputFile, originalOutputFile, runMetaFile *string,
		extraParams []string,
		extraMountPoints map[string]string,
	) (*RunMetadata, error)
}

// OmegajailSandbox is an implementation of a Sandbox that uses the omegajail
// sandbox.
type OmegajailSandbox struct {
	omegajailRoot string
}

// NewOmegajailSandbox creates a new OmegajailSandbox.
func NewOmegajailSandbox(omegajailRoot string) *OmegajailSandbox {
	return &OmegajailSandbox{
		omegajailRoot: omegajailRoot,
	}
}

// Supported returns whether the omegajail binary is installed in the system.
func (o *OmegajailSandbox) Supported() bool {
	_, err := os.Stat(path.Join(o.omegajailRoot, "bin/omegajail"))
	return err == nil
}

// Compile compiles the contestant-supplied program using the specified
// configuration using the omegajail sandbox.
func (o *OmegajailSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	if lang == "cs" {
		// C# needs to have a *.runtimeconfig.json file next to the file that is
		// being compiled.
		err := os.Symlink(
			"/usr/share/dotnet/Main.runtimeconfig.json",
			fmt.Sprintf("%s/%s.runtimeconfig.json", chdir, target),
		)
		if err != nil {
			ctx.Log.Error(
				"Failed to symlink runtimeconfig",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	}

	params := []string{
		"--homedir", chdir,
		"--homedir-writable",
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.FormatInt(int64(ctx.Config.Runner.CompileTimeLimit.Milliseconds()), 10),
		"-O", strconv.FormatInt(ctx.Config.Runner.CompileOutputLimit.Bytes(), 10),
		"--root", o.omegajailRoot,
		"--compile", lang,
		"--compile-target", target,
	}
	for _, inputFile := range inputFiles {
		if !strings.HasPrefix(inputFile, chdir) {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, errors.Errorf("file %q is not within the chroot", inputFile)
		}
		rel, err := filepath.Rel(chdir, inputFile)
		if err != nil {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, err
		}
		params = append(
			params,
			"--compile-source", rel,
		)
	}
	if len(extraFlags) > 0 {
		params = append(params, "--")
		params = append(params, extraFlags...)
	}

	invokeOmegajail(ctx, o.omegajailRoot, params, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	metadata, err := parseMetaFile(ctx, nil, lang, metaFd, &outputFile, nil, false)

	if lang == "java" && metadata.Verdict == "OK" {
		classPath := path.Join(chdir, fmt.Sprintf("%s.class", target))
		if _, err := os.Stat(classPath); os.IsNotExist(err) {
			compileError := fmt.Sprintf(
				"Class `%s` not found. Make sure your class is named `%s` "+
					"and outside all packages",
				target,
				target,
			)
			metadata.Verdict = "CE"
			f, err := os.OpenFile(errorFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
			if err != nil {
				return metadata, err
			}
			defer f.Close()
			f.WriteString("\n")
			f.WriteString(compileError)
		}
	}

	return metadata, err
}

// Run invokes the contestant-supplied program against a specified input and
// run configuration using the omegajail sandbox.
func (o *OmegajailSandbox) Run(
	ctx *common.Context,
	limits *common.LimitsSettings,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string,
	extraMountPoints map[string]string,
) (*RunMetadata, error) {
	timeLimit := limits.TimeLimit
	if lang == "java" {
		timeLimit += 1000
	}

	// Avoid using the real /dev/null. Pass in an empty file instead.
	if inputFile == "/dev/null" {
		inputFile = path.Join(o.omegajailRoot, "root/dev/null")
	}

	type fileLink struct {
		sourceFile, targetFile string
	}
	fileLinks := []fileLink{}
	if originalInputFile != nil {
		fileLinks = append(fileLinks, fileLink{
			sourceFile: *originalInputFile,
			targetFile: path.Join(chdir, "data.in"),
		})
	}
	if originalOutputFile != nil && *originalOutputFile != "/dev/null" {
		fileLinks = append(fileLinks, fileLink{
			sourceFile: *originalOutputFile,
			targetFile: path.Join(chdir, "data.out"),
		})
	}
	if runMetaFile != nil {
		fileLinks = append(fileLinks, fileLink{
			sourceFile: *runMetaFile,
			targetFile: path.Join(chdir, "meta.in"),
		})
	}
	for _, fl := range fileLinks {
		if _, err := os.Stat(fl.targetFile); err == nil {
			os.Remove(fl.targetFile)
		}
		if err := copyFile(fl.sourceFile, fl.targetFile); err != nil {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, err
		}
	}

	// Create intermediate directories, if needed.
	if err := os.MkdirAll(path.Dir(outputFile), 0o755); err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}

	// "640MB should be enough for anybody"
	hardLimit := base.MinBytes(base.Byte(640)*base.Mebibyte, limits.MemoryLimit)

	params := []string{
		"--homedir", chdir,
		"-0", inputFile,
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-m", strconv.FormatInt(hardLimit.Bytes(), 10),
		"-t", strconv.FormatInt(int64(timeLimit.Milliseconds()), 10),
		"-w", strconv.FormatInt(int64(limits.ExtraWallTime.Milliseconds()), 10),
		"-O", strconv.FormatInt(limits.OutputLimit.Bytes(), 10),
		"--root", o.omegajailRoot,
		"--run", lang,
		"--run-target", target,
	}
	for path, mountTarget := range extraMountPoints {
		params = append(
			params,
			"--bind", fmt.Sprintf("%s:%s", path, mountTarget),
		)
	}
	if len(extraParams) > 0 {
		params = append(params, "--")
		params = append(params, extraParams...)
	}

	preloader, err := newInputPreloader(inputFile)
	if err != nil {
		ctx.Log.Error(
			"Failed to preload input",
			map[string]interface{}{
				"file": inputFile,
				"err":  err,
			},
		)
	} else if preloader != nil {
		// preloader might be nil, even with no error.
		preloader.release()
	}

	invokeOmegajail(ctx, o.omegajailRoot, params, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	return parseMetaFile(ctx, limits, lang, metaFd, &outputFile, &errorFile, lang == "c")
}

func invokeOmegajail(ctx *common.Context, omegajailRoot string, omegajailParams []string, errorFile string) {
	omegajailFullParams := []string{path.Join(omegajailRoot, "bin/omegajail")}
	omegajailFullParams = append(omegajailFullParams, omegajailParams...)
	ctx.Log.Debug(
		"invoking",
		map[string]interface{}{
			"params": shellquote.Join(omegajailFullParams...),
		},
	)
	cmd := exec.Command(omegajailFullParams[0], omegajailParams...)
	omegajailErrorFile := errorFile + ".omegajail"
	omegajailErrorFd, err := os.Create(omegajailErrorFile)
	if err != nil {
		ctx.Log.Error(
			"Failed to redirect omegajail stderr",
			map[string]interface{}{
				"err": err,
			},
		)
	} else {
		defer os.Remove(omegajailErrorFile)
		cmd.Stderr = omegajailErrorFd
	}
	if err := cmd.Run(); err != nil {
		ctx.Log.Error(
			"Omegajail execution failed",
			map[string]interface{}{
				"err": err,
			},
		)
	}
	if omegajailErrorFd != nil {
		omegajailErrorFd.Close()
		if err := appendFile(errorFile, omegajailErrorFile); err != nil {
			ctx.Log.Error(
				"Failed to append omegajail stderr",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	}
}

func appendFile(dest, src string) error {
	srcFd, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFd.Close()
	stat, err := srcFd.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}
	destFd, err := os.OpenFile(dest, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer destFd.Close()
	destFd.WriteString("\n")
	_, err = io.Copy(destFd, srcFd)
	return err
}

func parseMetaFile(
	ctx *common.Context,
	limits *common.LimitsSettings,
	lang string,
	metaFile io.Reader,
	outputFilePath *string,
	errorFilePath *string,
	allowNonZeroExitCode bool,
) (*RunMetadata, error) {
	meta := &RunMetadata{
		Verdict:    "JE",
		ExitStatus: -1,
	}
	scanner := bufio.NewScanner(metaFile)
	for scanner.Scan() {
		tokens := strings.SplitN(scanner.Text(), ":", 2)
		if len(tokens) < 2 {
			return meta, errors.Errorf("malformed meta file line: %q", scanner.Text())
		}
		switch tokens[0] {
		case "status":
			meta.ExitStatus, _ = strconv.Atoi(tokens[1])
		case "time":
			meta.Time, _ = strconv.ParseFloat(tokens[1], 64)
			meta.Time /= 1e6
		case "time-sys":
			meta.SystemTime, _ = strconv.ParseFloat(tokens[1], 64)
			meta.SystemTime /= 1e6
		case "time-wall":
			meta.WallTime, _ = strconv.ParseFloat(tokens[1], 64)
			meta.WallTime /= 1e6
		case "mem":
			memoryBytes, _ := strconv.ParseInt(tokens[1], 10, 64)
			meta.Memory = base.Byte(memoryBytes)
		case "signal":
			meta.Signal = &tokens[1]
		case "signal_number":
			stringSignal := fmt.Sprintf("SIGNAL %s", tokens[1])
			meta.Signal = &stringSignal
		case "syscall":
			meta.Syscall = &tokens[1]
		case "syscall_number":
			stringSyscall := fmt.Sprintf("SYSCALL %s", tokens[1])
			meta.Syscall = &stringSyscall
		default:
			ctx.Log.Warn(
				"Unknown field in .meta file",
				map[string]interface{}{
					"tokens": tokens,
				},
			)
		}
	}
	if err := scanner.Err(); err != nil {
		return meta, err
	}

	if meta.Signal != nil {
		switch *meta.Signal {
		case "SIGSYS":
			meta.Verdict = "RFE"
		case "SIGILL", "SIGABRT", "SIGFPE", "SIGKILL", "SIGPIPE", "SIGBUS", "SIGSEGV":
			meta.Verdict = "RTE"
		case "SIGALRM", "SIGXCPU":
			meta.Verdict = "TLE"
		case "SIGXFSZ":
			meta.Verdict = "OLE"
		default:
			ctx.Log.Error(
				"Received odd signal",
				map[string]interface{}{
					"signal": *meta.Signal,
				},
			)
			meta.Verdict = "RTE"
		}
	} else if meta.ExitStatus == 0 || allowNonZeroExitCode {
		meta.Verdict = "OK"
	} else {
		meta.Verdict = "RTE"
	}

	if lang == "kj" || lang == "kp" {
		// Karel programs have a unique exit status per each one of the failure
		// modes. Map 1 (INSTRUCTION) to TLE.
		if meta.ExitStatus == 1 {
			meta.Verdict = "TLE"
		}
	}
	if limits != nil &&
		limits.MemoryLimit > 0 &&
		(meta.Memory > limits.MemoryLimit ||
			lang == "java" && meta.ExitStatus != 0 && isJavaMLE(ctx, errorFilePath)) {
		meta.Verdict = "MLE"
		meta.Memory = limits.MemoryLimit
	}

	if outputFilePath != nil {
		outputFileStat, err := os.Stat(*outputFilePath)
		if err == nil {
			meta.OutputSize = base.Byte(outputFileStat.Size())
		}
	}

	return meta, nil
}

func isJavaMLE(ctx *common.Context, errorFilePath *string) bool {
	if errorFilePath == nil {
		return false
	}

	f, err := os.Open(*errorFilePath)
	if err != nil {
		ctx.Log.Error(
			"Failed to open stderr",
			map[string]interface{}{
				"err": err,
			},
		)
		return false
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4096)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			break
		}
		if strings.Contains(string(line), "java.lang.OutOfMemoryError") {
			return true
		}
	}

	return false
}
