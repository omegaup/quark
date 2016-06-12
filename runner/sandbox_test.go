package runner

import (
	"bytes"
	"fmt"
	"github.com/lhchavez/quark/common"
	"os"
	"testing"
)

func TestMinijail(t *testing.T) {
	minijail := MinijailSandbox{}
	if !minijail.Supported() {
		t.Skip("minijail sandbox not supported")
	}
}

func TestParseMetaFile(t *testing.T) {
	ctx, err := newRunnerContext()
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
			fmt.Sprintf("status:0\nmem:%d", ctx.Config.Runner.JavaVmEstimatedSize),
			"c",
			nil,
			func(meta *RunMetadata) bool {
				return meta.Memory == ctx.Config.Runner.JavaVmEstimatedSize
			},
		},
		{
			fmt.Sprintf("status:0\nmem:%d", ctx.Config.Runner.JavaVmEstimatedSize),
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

func TestMinMax(t *testing.T) {
	test := []struct {
		a, b, expectMin, expectMax int64
	}{
		{0, 1, 0, 1},
		{1, 0, 0, 1},
	}
	for _, te := range test {
		gotMin := min64(te.a, te.b)
		if gotMin != te.expectMin {
			t.Errorf("gotMin(%q) == %d, expected %d", te, gotMin, te.expectMin)
		}
		gotMax := max64(te.a, te.b)
		if gotMax != te.expectMax {
			t.Errorf("gotMax(%q) == %d, expected %d", te, gotMax, te.expectMax)
		}
	}
}
