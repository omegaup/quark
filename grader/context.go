package grader

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/omegaup/quark/common"
)

// A Context holds the state of the Grader.
type Context struct {
	common.Context
	QueueManager          *QueueManager
	InflightMonitor       *InflightMonitor
	InputManager          *common.InputManager
	LibinteractiveVersion string
}

// GetLibinteractiveVersion returns the version of the installed libinteractive
// jar.
func GetLibinteractiveVersion() (string, error) {
	cmd := exec.Command(
		"/usr/bin/java",
		"-jar", "/usr/share/java/libinteractive.jar",
		"--version",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}

	rawOutput, err := ioutil.ReadAll(stdout)
	if err != nil {
		return "", err
	}

	tokens := strings.Split(strings.TrimSpace(string(rawOutput)), " ")
	return tokens[len(tokens)-1], nil
}

// NewContext returns a new Context where the configuration is read in a JSON
// format from the supplied io.Reader.
func NewContext(reader io.Reader) (*Context, error) {
	ctx, err := common.NewContextFromReader(reader, "grader")
	if err != nil {
		return nil, err
	}
	libinteractiveVersion, err := GetLibinteractiveVersion()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(ctx.Config.Grader.RuntimePath, 0755); err != nil {
		return nil, err
	}

	return &Context{
		Context: *ctx,
		QueueManager: NewQueueManager(
			ctx.Config.Grader.ChannelLength,
			ctx.Config.Grader.RuntimePath,
		),
		InflightMonitor:       NewInflightMonitor(),
		InputManager:          common.NewInputManager(ctx),
		LibinteractiveVersion: libinteractiveVersion,
	}, nil
}

// Close releases all resources owned by the context.
func (ctx *Context) Close() {
	ctx.QueueManager.Close()
}

// Wrap returns a new Context with the applied context.
func (ctx *Context) Wrap(c context.Context) *Context {
	wrapped := *ctx
	wrapped.Context = *wrapped.Context.Wrap(c)
	return &wrapped
}
