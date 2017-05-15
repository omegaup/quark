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
	minijailPath string = "/var/lib/minijail"
)

var (
	haskellCompiler string = "/usr/lib/ghc/bin/ghc"
)

func init() {
	// ghc was moved from lib/ghc to bin/ghc recently. Try to detect that.
	_, err := os.Stat(path.Join(minijailPath, "root-hs/lib/ghc"))
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

type MinijailSandbox struct{}

func (*MinijailSandbox) Supported() bool {
	_, err := os.Stat(path.Join(minijailPath, "bin/minijail0"))
	return err == nil
}

func (*MinijailSandbox) Compile(
	ctx *common.Context,
	lang string,
	inputFiles []string,
	chdir, outputFile, errorFile, metaFile, target string,
	extraFlags []string,
) (*RunMetadata, error) {
	commonParams := []string{
		path.Join(minijailPath, "bin/minijail0"),
		"-q",
		"-C", path.Join(minijailPath, "root-compilers"),
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
			"-S", path.Join(minijailPath, "scripts/javac"),
			"-b", path.Join(minijailPath, "root-openjdk,/usr/lib/jvm"),
			"-b", "/sys/,/sys",
			"--", "/usr/bin/javac", "-J-Xmx512M", "-d", ".",
		}
	case "c":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/gcc"),
			"--", "/usr/bin/gcc", "-o", target, "-std=c11", "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "cpp":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/gcc"),
			"--", "/usr/bin/g++", "-o", target, "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "cpp11":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/gcc"),
			"--", "/usr/bin/g++", "-o", target, "-std=c++11", "-O2",
		}
		linkerFlags = append(linkerFlags, "-lm")
	case "pas":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/fpc"),
			"--", "/usr/bin/ldwrapper", "/usr/bin/fpc", "-Tlinux", "-O2",
			"-Mobjfpc", "-Sc", "-Sh", fmt.Sprintf("-o%s", target),
		}
	case "py":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/pyc"),
			"-b", path.Join(minijailPath, "root-python") + ",/usr/lib/python2.7",
			"--", "/usr/bin/python", "-m", "py_compile",
		}
	case "rb":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/ruby"),
			"-b", path.Join(minijailPath, "root-ruby") + ",/usr/lib/ruby",
			"--", "/usr/bin/ruby", "-wc",
		}
	case "kj":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/js"),
			"-b", path.Join(minijailPath, "root-js") + ",/opt/nodejs",
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "compile", "java",
			"-o", fmt.Sprintf("%s.kx", target),
		}
	case "kp":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/js"),
			"-b", path.Join(minijailPath, "root-js") + ",/opt/nodejs",
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "compile", "pascal",
			"-o", fmt.Sprintf("%s.kx", target),
		}
	case "hs":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/ghc"),
			"-b", path.Join(minijailPath, "root-hs") + ",/usr/lib/ghc",
			"--", haskellCompiler, "-B/usr/lib/ghc", "-O2", "-o", target,
		}
	case "lua":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/lua"),
			"--", "/usr/bin/luac", "-o", target,
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, extraFlags...)
	finalParams = append(finalParams, inputFlags...)
	finalParams = append(finalParams, linkerFlags...)

	invokeMinijail(ctx, finalParams, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	metadata, err := parseMetaFile(ctx, nil, lang, metaFd, false)

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

func (*MinijailSandbox) Run(
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

	commonParams := []string{
		path.Join(minijailPath, "bin/minijail0"),
		"-q",
		"-C", path.Join(minijailPath, "root"),
		"-d", "/home",
		"-b", chdir + ",/home",
		"-0", inputFile,
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.FormatInt(timeLimit, 10),
		"-w", strconv.FormatInt(limits.ExtraWallTime, 10),
		"-O", strconv.FormatInt(limits.OutputLimit, 10),
		"-k", "-1",
	}

	extraMinijailFlags := make([]string, 2*len(extraMountPoints))
	i := 0
	for path, mountTarget := range extraMountPoints {
		extraMinijailFlags[i] = "-b"
		extraMinijailFlags[i+1] = fmt.Sprintf("%s,%s", path, mountTarget)
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
	memoryLimit := (16*1024 + limits.MemoryLimit) * 1024
	// "640MB should be enough for anybody"
	hardLimit := strconv.FormatInt(min64(640*1024*1024, memoryLimit), 10)

	var params []string

	switch lang {
	case "java":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/java"),
			"-b", path.Join(minijailPath, "root-openjdk,/usr/lib/jvm"),
			"-b", "/sys/,/sys",
			"--", "/usr/bin/java", fmt.Sprintf("-Xmx%d", memoryLimit), target,
		}
	case "c", "cpp", "cpp11":
		if limits.MemoryLimit != -1 {
			params = []string{
				"-S", path.Join(minijailPath, "scripts/cpp"),
				"-m", hardLimit,
			}
		} else {
			// It's dangerous to go without seccomp-bpf, but this is only for testing.
			params = []string{}
		}
		params = append(params, "--", fmt.Sprintf("./%s", target))
	case "pas":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/pas"),
			"-m", hardLimit, "--", "/usr/bin/ldwrapper", fmt.Sprintf("./%s", target),
		}
	case "py":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/py"),
			"-b", path.Join(minijailPath, "root-python") + ",/usr/lib/python2.7",
			"-m", hardLimit, "--", "/usr/bin/python", fmt.Sprintf("./%s.py", target),
		}
	case "rb":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/ruby"),
			"-b", path.Join(minijailPath, "root-ruby") + ",/usr/lib/ruby",
			"-m", hardLimit, "--", "/usr/bin/ruby", fmt.Sprintf("./%s.rb", target),
		}
	case "kp", "kj":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/js"),
			"-b", path.Join(minijailPath, "root-js") + ",/opt/nodejs",
			"--", "/usr/bin/node", "/opt/nodejs/karel.js", "run", fmt.Sprintf("%s.kx", target),
		}
	case "hs":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/hs"),
			"-b", path.Join(minijailPath, "root-hs") + ",/usr/lib/ghc",
			"-m", hardLimit, "--", fmt.Sprintf("./%s", target),
		}
	case "lua":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/lua"),
			"-m", hardLimit, "--", "/usr/bin/lua", target,
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, extraMinijailFlags...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, extraParams...)

	preloader, err := newInputPreloader(inputFile)
	if err != nil {
		ctx.Log.Error("Failed to preload input", "file", inputFile, "err", err)
	} else if preloader != nil {
		// preloader might be nil, even with no error.
		preloader.release()
	}

	invokeMinijail(ctx, finalParams, errorFile)
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return &RunMetadata{
			Verdict:    "JE",
			ExitStatus: -1,
		}, err
	}
	defer metaFd.Close()
	return parseMetaFile(ctx, limits, lang, metaFd, lang == "c")
}

func invokeMinijail(ctx *common.Context, minijailParams []string, errorFile string) {
	ctx.Log.Debug("invoking", "params", minijailParams)
	cmd := exec.Command("/usr/bin/sudo", minijailParams...)
	minijailErrorFile := errorFile + ".minijail"
	minijailErrorFd, err := os.Create(minijailErrorFile)
	if err != nil {
		ctx.Log.Error("Failed to redirect minijail stderr", "err", err)
	} else {
		defer os.Remove(minijailErrorFile)
		cmd.Stderr = minijailErrorFd
	}
	if err := cmd.Run(); err != nil {
		ctx.Log.Error(
			"Minijail execution failed",
			"err", err,
		)
	}
	if minijailErrorFd != nil {
		minijailErrorFd.Close()
		if err := appendFile(errorFile, minijailErrorFile); err != nil {
			ctx.Log.Error("Failed to append minijail stderr", "err", err)
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
	}
	if limits != nil &&
		limits.MemoryLimit > 0 &&
		meta.Memory > limits.MemoryLimit &&
		(lang != "java" || meta.ExitStatus != 0) {
		meta.Verdict = "MLE"
		meta.Memory = limits.MemoryLimit
	}

	return meta, nil
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
