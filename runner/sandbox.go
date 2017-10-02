package runner

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	omegajailPath string = "/var/lib/omegajail"
)

var (
	haskellCompiler string = "/usr/lib/ghc/bin/ghc"
)

func init() {
	// ghc was moved from lib/ghc to bin/ghc recently. Try to detect that.
	_, err := os.Stat(path.Join(omegajailPath, "root-hs/lib/ghc"))
	if !os.IsNotExist(err) {
		haskellCompiler = "/usr/lib/ghc/lib/ghc"
	}
}

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
	Verdict    string  `json:"verdict"`
	ExitStatus int     `json:"exit_status,omitempty"`
	Time       float64 `json:"time"`
	SystemTime float64 `json:"sys_time"`
	WallTime   float64 `json:"wall_time"`
	Memory     int64   `json:"memory"`
	Signal     *string `json:"signal,omitempty"`
	Syscall    *string `json:"syscall,omitempty"`
}

func (m *RunMetadata) String() string {
	metadata := fmt.Sprintf(
		"{Verdict: %s, ExitStatus: %d, Time: %.3fs, SystemTime: %.3fs, WallTime: %.3fs, Memory: %.3fMiB",
		m.Verdict,
		m.ExitStatus,
		m.Time,
		m.SystemTime,
		m.WallTime,
		float64(m.Memory)/1024.0/1024.0,
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

type OmegajailSandbox struct{}

func (*OmegajailSandbox) Supported() bool {
	_, err := os.Stat(path.Join(omegajailPath, "bin/omegajail"))
	return err == nil
}

func (*OmegajailSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	commonParams := []string{
		"-C", path.Join(omegajailPath, "root-compilers"),
		"-d", "/home",
		"-b", chdir + ",/home,1",
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.Itoa(ctx.Config.Runner.CompileTimeLimit * 1000),
		"-O", strconv.Itoa(ctx.Config.Runner.CompileOutputLimit),
	}

	inputFlags := make([]string, 0)

	for _, inputFile := range inputFiles {
		if !strings.HasPrefix(inputFile, chdir) {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, errors.New("file " + inputFile + " is not within the chroot")
		}
		rel, err := filepath.Rel(chdir, inputFile)
		if err != nil {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, err
		}
		inputFlags = append(inputFlags, rel)
	}

	var params []string
	linkerFlags := make([]string, 0)

	switch lang {
	case "java":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/javac"),
			"-b", path.Join(omegajailPath, "root-openjdk,/usr/lib/jvm"),
			"--", "/usr/bin/javac", "-J-Xmx512M", "-d", ".",
		}
	case "c":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/gcc"),
			"--", "/usr/bin/gcc", "-o", target, "-std=c11", "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "cpp":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/gcc"),
			"--", "/usr/bin/g++", "-o", target, "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "cpp11":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/gcc"),
			"--", "/usr/bin/g++", "-o", target, "-std=c++11", "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "pas":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/fpc"),
			"--", "/usr/bin/fpc", "-Tlinux", "-O2",
			"-Mobjfpc", "-Sc", "-Sh", fmt.Sprintf("-o%s", target),
		}
	case "py":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/pyc"),
			"-b", path.Join(omegajailPath, "root-python") + ",/usr/lib/python2.7",
			"--", "/usr/bin/python", "-m", "py_compile",
		}
	case "rb":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/ruby"),
			"-b", path.Join(omegajailPath, "root-ruby") + ",/usr/lib/ruby",
			"--", "/usr/bin/ruby", "-wc",
		}
	case "kj":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/js"),
			"-b", path.Join(omegajailPath, "root-js") + ",/opt/nodejs",
			"-0", path.Join(omegajailPath, "root-compilers/dev/null"),
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "compile", "java",
			"-o", fmt.Sprintf("%s.kx", target),
		}
	case "kp":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/js"),
			"-b", path.Join(omegajailPath, "root-js") + ",/opt/nodejs",
			"-0", path.Join(omegajailPath, "root-compilers/dev/null"),
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "compile", "pascal",
			"-o", fmt.Sprintf("%s.kx", target),
		}
	case "hs":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/ghc"),
			"-b", path.Join(omegajailPath, "root-hs") + ",/usr/lib/ghc",
			"--", haskellCompiler, "-B/usr/lib/ghc", "-O2", "-o", target,
		}
	case "lua":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/lua"),
			"--", "/usr/bin/luac", "-o", target,
		}
	case "cs":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/csc"),
			"-b", path.Join(omegajailPath, "root-dotnet") + ",/usr/share/dotnet",
			"--", "/usr/share/dotnet/dotnet",
			"/usr/share/dotnet/sdk/2.0.0/Roslyn/csc.exe", "/noconfig",
			"@/usr/share/dotnet/Release.rsp", "/target:exe",
			fmt.Sprintf("/out:%s.dll", target),
		}
		err := os.Symlink(
			"/usr/share/dotnet/Main.runtimeconfig.json",
			fmt.Sprintf("%s/%s.runtimeconfig.json", chdir, target),
		)
		if err != nil {
			ctx.Log.Error("Failed to symlink runtimeconfig", "err", err)
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, extraFlags...)
	finalParams = append(finalParams, inputFlags...)
	finalParams = append(finalParams, linkerFlags...)

	invokeOmegajail(ctx, finalParams, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	metadata, err := parseMetaFile(ctx, nil, lang, metaFd, nil, false)

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

func (*OmegajailSandbox) Run(
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
		inputFile = path.Join(omegajailPath, "root/dev/null")
	}

	commonParams := []string{
		"-C", path.Join(omegajailPath, "root"),
		"-d", "/home",
		"-b", chdir + ",/home",
		"-0", inputFile,
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.FormatInt(timeLimit, 10),
		"-w", strconv.FormatInt(limits.ExtraWallTime, 10),
		"-O", strconv.FormatInt(limits.OutputLimit, 10),
	}

	extraOmegajailFlags := make([]string, 2*len(extraMountPoints))
	i := 0
	for path, mountTarget := range extraMountPoints {
		extraOmegajailFlags[i] = "-b"
		extraOmegajailFlags[i+1] = fmt.Sprintf("%s,%s", path, mountTarget)
		i += 2
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
		if err := os.Link(fl.sourceFile, fl.targetFile); err != nil {
			return &RunMetadata{
				Verdict:    "JE",
				ExitStatus: -1,
			}, err
		}
	}

	// 16MB + memory limit to prevent some RTE
	memoryLimit := 16*1024*1024 + limits.MemoryLimit
	// "640MB should be enough for anybody"
	hardLimit := strconv.FormatInt(min64(640*1024*1024, memoryLimit), 10)

	var params []string

	switch lang {
	case "java":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/java"),
			"-b", path.Join(omegajailPath, "root-openjdk,/usr/lib/jvm"),
			"--", "/usr/bin/java", fmt.Sprintf("-Xmx%d", memoryLimit), target,
		}
	case "c", "cpp", "cpp11":
		if limits.MemoryLimit != -1 {
			params = []string{
				"-S", path.Join(omegajailPath, "scripts/cpp"),
				"-m", hardLimit,
			}
		} else {
			// It's dangerous to go without seccomp-bpf, but this is only for testing.
			params = []string{}
		}
		params = append(params, "--", fmt.Sprintf("./%s", target))
	case "pas":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/pas"),
			"-m", hardLimit, "--", fmt.Sprintf("./%s", target),
		}
	case "py":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/py"),
			"-b", path.Join(omegajailPath, "root-python") + ",/usr/lib/python2.7",
			"-m", hardLimit, "--", "/usr/bin/python", fmt.Sprintf("./%s.py", target),
		}
	case "rb":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/ruby"),
			"-b", path.Join(omegajailPath, "root-ruby") + ",/usr/lib/ruby",
			"-m", hardLimit, "--", "/usr/bin/ruby", fmt.Sprintf("./%s.rb", target),
		}
	case "kp", "kj":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/js"),
			"-b", path.Join(omegajailPath, "root-js") + ",/opt/nodejs",
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "run", fmt.Sprintf("%s.kx", target),
		}
	case "hs":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/hs"),
			"-b", path.Join(omegajailPath, "root-hs") + ",/usr/lib/ghc",
			"-m", hardLimit, "--", fmt.Sprintf("./%s", target),
		}
	case "lua":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/lua"),
			"-m", hardLimit, "--", "/usr/bin/lua", target,
		}
	case "cs":
		params = []string{
			"-S", path.Join(omegajailPath, "scripts/cs"),
			"-b", path.Join(omegajailPath, "root-dotnet") + ",/usr/share/dotnet",
			"--cgroup-memory-limit", hardLimit,
			"--", "/usr/share/dotnet/dotnet", fmt.Sprintf("%s.dll", target),
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, extraOmegajailFlags...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, extraParams...)

	preloader, err := newInputPreloader(inputFile)
	if err != nil {
		ctx.Log.Error("Failed to preload input", "file", inputFile, "err", err)
	} else if preloader != nil {
		// preloader might be nil, even with no error.
		preloader.release()
	}

	invokeOmegajail(ctx, finalParams, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	return parseMetaFile(ctx, limits, lang, metaFd, &errorFile, lang == "c")
}

func invokeOmegajail(ctx *common.Context, omegajailParams []string, errorFile string) {
	omegajailFullParams := []string{path.Join(omegajailPath, "bin/omegajail")}
	omegajailFullParams = append(omegajailFullParams, omegajailParams...)
	ctx.Log.Debug("invoking", "params", omegajailFullParams)
	cmd := exec.Command(omegajailFullParams[0], omegajailParams...)
	omegajailErrorFile := errorFile + ".omegajail"
	omegajailErrorFd, err := os.Create(omegajailErrorFile)
	if err != nil {
		ctx.Log.Error("Failed to redirect omegajail stderr", "err", err)
	} else {
		defer os.Remove(omegajailErrorFile)
		cmd.Stderr = omegajailErrorFd
	}
	if err := cmd.Run(); err != nil {
		ctx.Log.Error(
			"Omegajail execution failed",
			"err", err,
		)
	}
	if omegajailErrorFd != nil {
		omegajailErrorFd.Close()
		if err := appendFile(errorFile, omegajailErrorFile); err != nil {
			ctx.Log.Error("Failed to append omegajail stderr", "err", err)
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
			meta.Memory, _ = strconv.ParseInt(tokens[1], 10, 64)
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
			ctx.Log.Warn("Unknown field in .meta file", "tokens", tokens)
		}
	}
	if err := scanner.Err(); err != nil {
		return meta, err
	}

	if meta.Signal != nil {
		switch *meta.Signal {
		case "SIGILL", "SIGSYS":
			meta.Verdict = "RFE"
		case "SIGABRT", "SIGFPE", "SIGKILL", "SIGPIPE", "SIGBUS", "SIGSEGV":
			meta.Verdict = "RTE"
		case "SIGALRM", "SIGXCPU":
			meta.Verdict = "TLE"
		case "SIGXFSZ":
			meta.Verdict = "OLE"
		default:
			ctx.Log.Error("Received odd signal", "signal", *meta.Signal)
			meta.Verdict = "RTE"
		}
	} else if meta.ExitStatus == 0 || allowNonZeroExitCode {
		meta.Verdict = "OK"
	} else {
		meta.Verdict = "RTE"
	}

	if lang == "java" {
		meta.Memory = max64(0, meta.Memory-ctx.Config.Runner.JavaVmEstimatedSize)
	} else if lang == "cs" {
		meta.Memory = max64(0, meta.Memory-ctx.Config.Runner.ClrVmEstimatedSize)
	}
	if limits != nil &&
		limits.MemoryLimit > 0 &&
		(meta.Memory > limits.MemoryLimit ||
			lang == "java" && meta.ExitStatus != 0 && isJavaMLE(ctx, errorFilePath)) {
		meta.Verdict = "MLE"
		meta.Memory = limits.MemoryLimit
	}

	return meta, nil
}

func isJavaMLE(ctx *common.Context, errorFilePath *string) bool {
	if errorFilePath == nil {
		return false
	}

	f, err := os.Open(*errorFilePath)
	if err != nil {
		ctx.Log.Error("Failed to open stderr", "err", err)
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

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
