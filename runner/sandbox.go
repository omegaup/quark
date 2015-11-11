package runner

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/omegaup/quark/common"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	minijailPath string = "/var/lib/minijail"
)

type RunMetadata struct {
	Verdict    string  `json:"verdict"`
	ExitStatus int     `json:"exit_status,omitempty"`
	Time       float64 `json:"time"`
	WallTime   float64 `json:"wall_time"`
	Memory     int     `json:"memory"`
	Signal     *string `json:"signal,omitempty"`
	Syscall    *string `json:"syscall,omitempty"`
}

func Compile(ctx *common.Context, lang string, inputFiles []string, chdir,
	outputFile, errorFile, metaFile, target string,
	extraFlags []string) (*RunMetadata, error) {
	commonParams := []string{
		path.Join(minijailPath, "bin/minijail0"),
		"-C", path.Join(minijailPath, "root-compilers"),
		"-d", "/home",
		"-b", chdir + ",/home,1",
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.Itoa(ctx.Config.Runner.CompileTimeLimit * 1000),
		"-O", strconv.Itoa(ctx.Config.Runner.CompileOutputLimit),
	}

	chrootedInputFiles := make([]string, len(inputFiles))

	for i, inputFile := range inputFiles {
		if !strings.HasPrefix(inputFile, chdir) {
			return nil, errors.New("file " + inputFile + " is not within the chroot")
		}
		rel, err := filepath.Rel(chdir, inputFile)
		if err != nil {
			return nil, err
		}
		chrootedInputFiles[i] = rel
	}

	var params []string

	switch lang {
	case "java":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/javac"),
			"-b", path.Join(minijailPath, "root-openjdk,/usr/lib/jvm"),
			"-b", "/sys/,/sys",
			"--", "/usr/bin/javac", "-J-Xmx512M",
		}
	case "c":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/gcc"),
			"--", "/usr/bin/gcc", "-lm", "-o", target, "-std=c11", "-O2",
		}
	case "cpp", "cpp11":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/gcc"),
			"--", "/usr/bin/g++", "-lm", "-o", target, "-xc++", "-std=c++11", "-O2",
		}
	case "pas":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/fpc"),
			"--", "/usr/bin/ldwrapper", "/usr/bin/fpc", "-Tlinux", "-O2",
			"-Mobjfpc", "-Sc", "-Sh", "-o", target,
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
			"-S", path.Join(minijailPath, "scripts/kcl"),
			"--", "/usr/bin/ldwrapper", "/usr/bin/kcl", "-lj",
			"-o", fmt.Sprintf("%s.kx", target), "-c",
		}
	case "kp":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/kcl"),
			"--", "/usr/bin/ldwrapper", "/usr/bin/kcl", "-lp",
			"-o", fmt.Sprintf("%s.kx", target), "-c",
		}
	case "hs":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/ghc"),
			"-b", path.Join(minijailPath, "root-hs") + ",/usr/lib/ghc",
			"--", "/usr/bin/ghc", "-B/usr/lib/ghc", "-O2", "-o", target,
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, chrootedInputFiles...)

	ctx.Log.Debug("invoking minijail", "params", finalParams)

	_ = exec.Command("/usr/bin/sudo", finalParams...).Run()
	return parseMetaFile(ctx, nil, lang, metaFile)
}

func Run(ctx *common.Context, input common.Input,
	lang, chdir, inputFile, outputFile, errorFile, metaFile, target string,
	originalInputFile, originalOutputFile, runMetaFile *string,
	extraParams []string, extraMountPoints map[string]string) (*RunMetadata, error) {
	timeLimit := input.Settings().Limits.TimeLimit
	if lang == "java" {
		timeLimit += 1000
	}

	commonParams := []string{
		path.Join(minijailPath, "bin/minijail0"),
		"-C", path.Join(minijailPath, "root"),
		"-d", "/home",
		"-b", chdir + ",/home",
		"-0", inputFile,
		"-1", outputFile,
		"-2", errorFile,
		"-M", metaFile,
		"-t", strconv.Itoa(timeLimit),
		"-w", strconv.Itoa(input.Settings().Limits.ExtraWallTime),
		"-O", strconv.Itoa(input.Settings().Limits.OutputLimit),
		"-k", strconv.Itoa(input.Settings().Limits.StackLimit),
	}

	extraMinijailFlags := make([]string, 2*len(extraMountPoints))
	i := 0
	for path, target := range extraMountPoints {
		extraMinijailFlags[i] = "-b"
		extraMinijailFlags[i+1] = fmt.Sprintf("%s,%s", path, target)
		i += 2
	}

	if originalInputFile != nil {
		os.Link(*originalInputFile, path.Join(chdir, "data.in"))
		defer os.Remove(path.Join(chdir, "data.in"))
	}
	if originalOutputFile != nil {
		os.Link(*originalOutputFile, path.Join(chdir, "data.out"))
		defer os.Remove(path.Join(chdir, "data.out"))
	}
	if runMetaFile != nil {
		os.Link(*runMetaFile, path.Join(chdir, "meta.in"))
		defer os.Remove(path.Join(chdir, "meta.in"))
	}

	// 16MB + memory limit to prevent some RTE
	memoryLimit := (16*1024 + input.Settings().Limits.MemoryLimit) * 1024
	// "640MB should be enough for anybody"
	hardLimit := strconv.Itoa(min(640*1024*1024, memoryLimit))

	var params []string

	switch lang {
	case "java":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/java"),
			"-b", path.Join(minijailPath, "root-openjdk,/usr/lib/jvm"),
			"-b", "/sys/,/sys",
			"--", "/usr/bin/java", fmt.Sprintf("-Xmx%s", memoryLimit), target,
		}
	case "c", "cpp", "cpp11":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/cpp"),
			"-m", hardLimit, "--", fmt.Sprintf("./%s", target),
		}
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
			"-S", path.Join(minijailPath, "scripts/karel"),
			"--", "/usr/bin/ldwrapper", "/usr/bin/karel", "/dev/stdin", "-oi", "-q",
			"-p2", fmt.Sprintf("./%s.kx", target),
		}
	case "hs":
		params = []string{
			"-S", path.Join(minijailPath, "scripts/hs"),
			"-b", path.Join(minijailPath, "root-hs") + ",/usr/lib/ghc",
			"-m", hardLimit, "--", fmt.Sprintf("./%s", target),
		}
	}

	finalParams := make([]string, 0)
	finalParams = append(finalParams, commonParams...)
	finalParams = append(finalParams, extraMinijailFlags...)
	finalParams = append(finalParams, params...)
	finalParams = append(finalParams, extraParams...)

	ctx.Log.Debug("invoking minijail", "params", finalParams)

	_ = exec.Command("/usr/bin/sudo", finalParams...).Run()
	return parseMetaFile(ctx, input, lang, metaFile)
}

func parseMetaFile(ctx *common.Context, input common.Input,
	lang, metaFile string) (*RunMetadata, error) {
	meta := &RunMetadata{
		Verdict:    "JE",
		ExitStatus: -1,
	}
	metaFd, err := os.Open(metaFile)
	if err != nil {
		return meta, err
	}
	defer metaFd.Close()
	scanner := bufio.NewScanner(metaFd)
	for scanner.Scan() {
		tokens := strings.SplitN(scanner.Text(), ":", 2)
		switch tokens[0] {
		case "status":
			meta.ExitStatus, _ = strconv.Atoi(tokens[1])
		case "time":
			meta.Time, _ = strconv.ParseFloat(tokens[1], 64)
			meta.Time /= 1e6
		case "time-wall":
			meta.WallTime, _ = strconv.ParseFloat(tokens[1], 64)
			meta.WallTime /= 1e6
		case "mem":
			meta.Memory, _ = strconv.Atoi(tokens[1])
		case "signal":
			meta.Signal = &tokens[1]
		case "signal_number":
			stringSignal := fmt.Sprintf("SIGNAL %s", tokens[1])
			meta.Signal = &stringSignal
		case "syscall":
			meta.Syscall = &tokens[1]
		case "syscall_number":
			stringSyscall := fmt.Sprintf("SYSCALLL %s", tokens[1])
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
	} else if meta.ExitStatus == 0 || lang == "c" {
		meta.Verdict = "OK"
	} else {
		meta.Verdict = "RTE"
	}

	if input != nil {
		if lang == "java" {
			meta.Memory -= ctx.Config.Runner.JavaVmEstimatedSize
		} else if meta.Memory > input.Settings().Limits.MemoryLimit {
			meta.Verdict = "MLE"
			meta.Memory = input.Settings().Limits.MemoryLimit
		}
	}

	return meta, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
