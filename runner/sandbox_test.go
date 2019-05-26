package runner

import (
	"bytes"
	"fmt"
	"github.com/omegaup/quark/common"
	"os"
	"testing"
)

func TestOmegajail(t *testing.T) {
	omegajail := OmegajailSandbox{}
	if !omegajail.Supported() {
		t.Skip("omegajail sandbox not supported")
	}
}

func TestParseMetaFile(t *testing.T) {
	ctx, err := newRunnerContext(t)
	if err != nil {
		t.Fatalf("RunnerContext creation failed with %q", err)
	}
	defer ctx.Close()
	defer os.RemoveAll(ctx.Config.Runner.RuntimePath)

	test := []struct {
		contents, lang string
		limits         *common.LimitsSettings
		callback       func(meta *RunMetadata) bool
	}{
		{
			"status:0",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "OK" },
		},
		{
			"status:1",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "OK" },
		},
		{
			"status:1",
			"cpp",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "RTE" },
		},
		{
			fmt.Sprintf("status:0\nmem:%d", ctx.Config.Runner.JavaVMEstimatedSize),
			"c",
			nil,
			func(meta *RunMetadata) bool {
				return meta.Memory == ctx.Config.Runner.JavaVMEstimatedSize
			},
		},
		{
			fmt.Sprintf("status:0\nmem:%d", ctx.Config.Runner.JavaVMEstimatedSize),
			"java",
			nil,
			func(meta *RunMetadata) bool { return meta.Memory == 0 },
		},
		{
			"status:0\ntime:1000000",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Time == 1 },
		},
		{
			"status:0\ntime-wall:1000000",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.WallTime == 1 },
		},
		{
			"status:0\ninvalid:field",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "OK" },
		},
		{
			"status:0\nsignal:SIGILL",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "RFE" },
		},
		{
			"status:0\nsignal:SIGABRT",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "RTE" },
		},
		{
			"status:0\nsignal:SIGXCPU",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "TLE" },
		},
		{
			"status:0\nsignal:SIGXFSZ",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "OLE" },
		},
		{
			"status:0\nsignal_number:0",
			"c",
			nil,
			func(meta *RunMetadata) bool {
				return meta.Verdict == "RTE" && *meta.Signal == "SIGNAL 0"
			},
		},
		{
			"status:0\nsyscall:futex",
			"c",
			nil,
			func(meta *RunMetadata) bool { return *meta.Syscall == "futex" },
		},
		{
			"status:0\nsyscall_number:0",
			"c",
			nil,
			func(meta *RunMetadata) bool { return *meta.Syscall == "SYSCALL 0" },
		},
		{
			"status:0\nmem:1000000",
			"c",
			nil,
			func(meta *RunMetadata) bool { return meta.Verdict == "OK" },
		},
		{
			"status:0\nmem:1000000",
			"c",
			&common.LimitsSettings{MemoryLimit: 1000},
			func(meta *RunMetadata) bool {
				return meta.Verdict == "MLE" && meta.Memory == 1000
			},
		},
	}
	for _, te := range test {
		meta, err := parseMetaFile(
			ctx,
			te.limits,
			te.lang,
			bytes.NewBufferString(te.contents),
			nil,
			te.lang == "c",
		)
		if err != nil {
			t.Errorf("Parsing meta file failed: %q", err)
			continue
		}
		if !te.callback(meta) {
			t.Errorf("Test failed for %v: %v", te, meta)
		}
	}
}
