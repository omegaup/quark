package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/inconshreveable/log15"
	"io"
	"os"
	"time"
)

// BroadcasterConfig represents the configuration for the Broadcaster.
type BroadcasterConfig struct {
	ChannelLength           int
	EventsPort              uint16
	FrontendURL             string
	PingPeriod              int64
	Port                    uint16
	Proxied                 bool
	ScoreboardUpdateSecret  string
	ScoreboardUpdateTimeout int64     // seconds
	TLS                     TLSConfig // only used if Proxied == false
	WriteDeadline           time.Duration
}

// InputManagerConfig represents the configuration for the InputManager.
type InputManagerConfig struct {
	CacheSize int64
}

// V1Config represents the configuration for the V1-compatibility shim for the
// Grader.
type V1Config struct {
	Enabled          bool
	Port             uint16
	RuntimeGradePath string
	RuntimePath      string
	SendBroadcast    bool
	UpdateDatabase   bool
	WriteResults     bool
}

// GraderConfig represents the configuration for the Grader.
type GraderConfig struct {
	ChannelLength   int
	Port            uint16
	RuntimePath     string
	MaxGradeRetries int
	BroadcasterURL  string
	V1              V1Config
	WriteGradeFiles bool // TODO(lhchavez): Remove once migration is done.
}

// TLSConfig represents the configuration for TLS.
type TLSConfig struct {
	CertFile string
	KeyFile  string
}

// RunnerConfig represents the configuration for the Runner.
type RunnerConfig struct {
	GraderURL           string
	RuntimePath         string
	CompileTimeLimit    int
	CompileOutputLimit  int
	ClrVMEstimatedSize  int64
	JavaVMEstimatedSize int64
	PreserveFiles       bool
}

// DbConfig represents the configuration for the database.
type DbConfig struct {
	Driver         string
	DataSourceName string
}

// TracingConfig represents the configuration for tracing.
type TracingConfig struct {
	Enabled bool
	File    string
}

// LoggingConfig represents the configuration for logging.
type LoggingConfig struct {
	File  string
	Level string
}

// MetricsConfig represents the configuration for metrics.
type MetricsConfig struct {
	Port uint16
}

// Config represents the configuration for the whole program.
type Config struct {
	Broadcaster  BroadcasterConfig
	InputManager InputManagerConfig
	Grader       GraderConfig
	Db           DbConfig
	Logging      LoggingConfig
	Metrics      MetricsConfig
	Tracing      TracingConfig
	Runner       RunnerConfig
	TLS          TLSConfig
}

var defaultConfig = Config{
	Broadcaster: BroadcasterConfig{
		ChannelLength:           10,
		EventsPort:              22291,
		FrontendURL:             "https://omegaup.com",
		PingPeriod:              30,
		Port:                    32672,
		Proxied:                 true,
		ScoreboardUpdateSecret:  "secret",
		ScoreboardUpdateTimeout: 10,
		TLS: TLSConfig{
			CertFile: "/etc/omegaup/broadcaster/certificate.pem",
			KeyFile:  "/etc/omegaup/broadcaster/key.pem",
		},
		WriteDeadline: time.Duration(5) * time.Second,
	},
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
	Metrics: MetricsConfig{
		Port: 6060,
	},
	Grader: GraderConfig{
		BroadcasterURL:  "https://omegaup.com:32672/broadcast/",
		ChannelLength:   1024,
		Port:            11302,
		RuntimePath:     "/var/lib/omegaup/",
		MaxGradeRetries: 3,
		V1: V1Config{
			Enabled:          false,
			Port:             21680,
			RuntimeGradePath: "/var/lib/omegaup/grade",
			RuntimePath:      "/var/lib/omegaup/",
			SendBroadcast:    true,
			UpdateDatabase:   true,
			WriteResults:     true,
		},
		WriteGradeFiles: true,
	},
	Runner: RunnerConfig{
		RuntimePath:         "/var/lib/omegaup/runner",
		GraderURL:           "https://omegaup.com:11302",
		CompileTimeLimit:    30,
		CompileOutputLimit:  10 * 1024 * 1024, // 10 MiB
		ClrVMEstimatedSize:  20 * 1024 * 1024, // 20 MiB
		JavaVMEstimatedSize: 30 * 1024 * 1024, // 30 MiB
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

// A Context holds data associated with a single request.
type Context struct {
	Config          Config
	Log             log15.Logger
	EventCollector  EventCollector
	EventFactory    *EventFactory
	Metrics         Metrics
	handler         log15.Handler
	logBuffer       *bytes.Buffer
	tracingFd       *os.File
	memoryCollector *MemoryEventCollector
}

// DefaultConfig returns a default Config.
func DefaultConfig() Config {
	return defaultConfig
}

// NewConfig creates a new Config from the specified reader.
func NewConfig(reader io.Reader) (*Config, error) {
	config := defaultConfig

	// Read basic config
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// NewContext creates a new Context from the specified Config. This also
// creates a Logger.
func NewContext(config *Config) (*Context, error) {
	var context = Context{
		Config:  *config,
		Metrics: &NoOpMetrics{},
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

// NewContextFromReader creates a new Context from the specified reader. This
// also creates a Logger.
func NewContextFromReader(reader io.Reader) (*Context, error) {
	config, err := NewConfig(reader)
	if err != nil {
		return nil, err
	}
	return NewContext(config)
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
func (context *Context) DebugContext(logCtx ...interface{}) *Context {
	var buffer bytes.Buffer
	childContext := &Context{
		Config:    context.Config,
		Log:       context.Log.New(logCtx...),
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

// AppendLogSection adds a complete section of logs to the log buffer. This
// typcally comes from a client.
func (context *Context) AppendLogSection(sectionName string, contents []byte) {
	fmt.Fprintf(context.logBuffer, "================  %s  ================\n", sectionName)
	context.logBuffer.Write(contents)
	fmt.Fprintf(context.logBuffer, "================ /%s  ================\n", sectionName)
}

// LogBuffer returns the contents of the logging buffer for this context.
func (context *Context) LogBuffer() []byte {
	if context.logBuffer == nil {
		return nil
	}
	return context.logBuffer.Bytes()
}

// TraceBuffer returns a JSON representation of the Trace Event stream for this
// Context.
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
