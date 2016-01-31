package common

import (
	"bytes"
	"encoding/json"
	"github.com/inconshreveable/log15"
	"io"
	"os"
)

// Configuration
type InputManagerConfig struct {
	CacheSize int64
}

type GraderConfig struct {
	ChannelLength   int
	Port            uint16
	RuntimePath     string
	MaxGradeRetries int
}

type TLSConfig struct {
	CertFile string
	KeyFile  string
}

type RunnerConfig struct {
	GraderURL           string
	RuntimePath         string
	CompileTimeLimit    int
	CompileOutputLimit  int
	JavaVmEstimatedSize int
	PreserveFiles       bool
}

type DbConfig struct {
	Driver         string
	DataSourceName string
}

type TracingConfig struct {
	Enabled bool
	File    string
}

type LoggingConfig struct {
	File  string
	Level string
}

type Config struct {
	InputManager InputManagerConfig
	Grader       GraderConfig
	Db           DbConfig
	Logging      LoggingConfig
	Tracing      TracingConfig
	Runner       RunnerConfig
	TLS          TLSConfig
}

var defaultConfig = Config{
	Db: DbConfig{
		Driver:         "sqlite3",
		DataSourceName: "./omegaup.db",
	},
	InputManager: InputManagerConfig{
		CacheSize: 1024 * 1024 * 1024, // 1 GiB
	},
	Logging: LoggingConfig{
		File:  "/var/log/omegaup/service.log",
		Level: "info",
	},
	Grader: GraderConfig{
		ChannelLength:   1024,
		Port:            11302,
		RuntimePath:     "/var/lib/omegaup/",
		MaxGradeRetries: 3,
	},
	Runner: RunnerConfig{
		RuntimePath:         "/var/lib/omegaup/runner",
		GraderURL:           "https://omegaup.com:11302",
		CompileTimeLimit:    30,
		CompileOutputLimit:  10 * 1024 * 1024, // 10 MiB
		JavaVmEstimatedSize: 14 * 1024 * 1024, // 14 MiB
		PreserveFiles:       false,
	},
	TLS: TLSConfig{
		CertFile: "/etc/omegaup/grader/certificate.pem",
		KeyFile:  "/etc/omegaup/grader/key.pem",
	},
	Tracing: TracingConfig{
		Enabled: true,
		File:    "/var/log/omegaup/tracing.json",
	},
}

func (config *Config) String() string {
	buf, err := json.MarshalIndent(*config, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(buf)
}

// Context
type Context struct {
	Config          Config
	Log             log15.Logger
	EventCollector  EventCollector
	EventFactory    *EventFactory
	handler         log15.Handler
	logBuffer       *bytes.Buffer
	tracingFd       *os.File
	memoryCollector *MemoryEventCollector
}

// NewContext creates a new Context from the specified reader. This also
// creates a Logger.
func NewContext(reader io.Reader) (*Context, error) {
	var context = Context{
		Config: defaultConfig,
	}

	// Read basic config
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&context.Config); err != nil {
		return nil, err
	}

	// Logging
	context.Log = log15.New()
	if context.Config.Logging.File == "/dev/null" {
		context.handler = log15.DiscardHandler()
	} else if context.Config.Logging.File == "stderr" {
		context.handler = log15.StderrHandler
	} else {
		handler, err := log15.FileHandler(
			context.Config.Logging.File,
			log15.LogfmtFormat(),
		)
		if err != nil {
			return nil, err
		}
		context.handler = handler
	}
	level, err := log15.LvlFromString(context.Config.Logging.Level)
	if err != nil {
		return nil, err
	}
	context.handler = log15.LvlFilterHandler(level, context.handler)
	context.Log.SetHandler(context.handler)

	// Tracing
	if context.Config.Tracing.Enabled {
		s, err := os.Stat(context.Config.Tracing.File)
		if os.IsNotExist(err) || s.Size() == 0 {
			context.tracingFd, err = os.Create(context.Config.Tracing.File)
			if err != nil {
				return nil, err
			}
			context.EventCollector, err = NewWriterEventCollector(
				context.tracingFd,
				false,
			)
			if err != nil {
				return nil, err
			}
		} else {
			context.tracingFd, err = os.OpenFile(
				context.Config.Tracing.File,
				os.O_WRONLY|os.O_APPEND,
				0664,
			)
			if err != nil {
				return nil, err
			}
			context.EventCollector, err = NewWriterEventCollector(
				context.tracingFd,
				true,
			)
			if err != nil {
				return nil, err
			}
		}
	} else {
		context.EventCollector = &NullEventCollector{}
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "main"
	}
	context.EventFactory = NewEventFactory(hostname, "main")
	context.EventFactory.Register(context.EventCollector)

	return &context, nil
}

// Close releases all resources owned by the context.
func (context *Context) Close() {
	if context.tracingFd != nil {
		context.tracingFd.Close()
	}
}

// DebugContext returns a new Context with an additional handler with a more
// verbose filter (using the Debug level) and a Buffer in which all logging
// statements will be (also) written to.
func (context *Context) DebugContext() *Context {
	var buffer bytes.Buffer
	childContext := &Context{
		Config:    context.Config,
		Log:       context.Log.New(),
		logBuffer: &buffer,
		handler: log15.MultiHandler(
			log15.StreamHandler(&buffer, log15.LogfmtFormat()),
			context.handler,
		),
		memoryCollector: NewMemoryEventCollector(),
		EventFactory:    context.EventFactory,
	}
	childContext.EventCollector = NewMultiEventCollector(
		childContext.memoryCollector,
		context.EventCollector,
	)
	childContext.EventFactory.Register(childContext.memoryCollector)
	childContext.Log.SetHandler(childContext.handler)
	return childContext
}

func (context *Context) LogBuffer() []byte {
	if context.logBuffer == nil {
		return nil
	}
	return context.logBuffer.Bytes()
}

func (context *Context) TraceBuffer() []byte {
	if context.memoryCollector == nil {
		return nil
	}
	data, err := json.Marshal(context.memoryCollector)
	if err != nil {
		return []byte(err.Error())
	}
	return data
}
