package grader

import (
	"github.com/lhchavez/quark/common"
	"io"
)

type Context struct {
	common.Context
	QueueManager    *QueueManager
	InflightMonitor *InflightMonitor
	InputManager    *common.InputManager
}

func NewContext(reader io.Reader) (*Context, error) {
	ctx, err := common.NewContextFromReader(reader)
	if err != nil {
		return nil, err
	}
	var context = &Context{
		Context:         *ctx,
		QueueManager:    NewQueueManager(ctx.Config.Grader.ChannelLength),
		InflightMonitor: NewInflightMonitor(),
		InputManager:    common.NewInputManager(ctx),
	}

	return context, nil
}

// Close releases all resources owned by the context.
func (context *Context) Close() {
}
