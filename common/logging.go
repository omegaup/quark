package common

import (
	"github.com/inconshreveable/log15"
	"io"
	"os"
	"syscall"
)

type closeableHandler struct {
	handler log15.Handler
	closer  func() error
}

var _ io.Closer = &closeableHandler{}
var _ log15.Handler = &closeableHandler{}

func (h *closeableHandler) Close() error {
	return h.closer()
}

func (h *closeableHandler) Log(r *log15.Record) error {
	return h.handler.Log(r)
}

func nopCloser() error { return nil }

// RotatingLog opens a log15.Logger, and if it will be pointed to a real file,
// it installs a SIGHUP handler that will atomically reopen the file and
// redirect all future logging operations.
func RotatingLog(config LoggingConfig) (log15.Logger, error) {
	log := log15.New()
	var handler log15.Handler
	closer := nopCloser
	if config.File == "/dev/null" {
		handler = log15.DiscardHandler()
	} else if config.File == "stderr" {
		handler = log15.StderrHandler
	} else {
		loggingFile, err := NewRotatingFile(
			config.File,
			0644,
			func(f *os.File, isEmpty bool) error {
				return syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
			},
		)
		if err != nil {
			return nil, err
		}

		fmtr := log15.LogfmtFormat()
		handler = log15.FuncHandler(func(r *log15.Record) error {
			_, err := loggingFile.Write(fmtr.Format(r))
			return err
		})
		handler = log15.LazyHandler(handler)
		closer = func() error { return loggingFile.Close() }
	}

	// Don't log things that are chattier than config.Level, but for errors also
	// include the stack trace.
	maxLvl, err := log15.LvlFromString(config.Level)
	if err != nil {
		return nil, err
	}
	log.SetHandler(&closeableHandler{
		handler: ErrorCallerStackHandler(maxLvl, handler),
		closer:  closer,
	})
	return log, nil
}

// ErrorCallerStackHandler creates a handler that drops all logs that are less
// important than maxLvl, and also adds a stack trace to all events that are
// errors / critical.
func ErrorCallerStackHandler(maxLvl log15.Lvl, handler log15.Handler) log15.Handler {
	callerStackHandler := log15.CallerStackHandler("%+v", handler)
	return log15.FuncHandler(func(r *log15.Record) error {
		if r.Lvl > maxLvl {
			return nil
		}
		if r.Lvl <= log15.LvlError {
			return callerStackHandler.Log(r)
		}
		return handler.Log(r)
	})
}
