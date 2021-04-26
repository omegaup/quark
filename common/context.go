package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/inconshreveable/log15"
	base "github.com/omegaup/go-base/v2"
	"io"
	"os"
	"time"
)

// BroadcasterConfig represents the configuration for the Broadcaster.
type BroadcasterConfig struct {
	ChannelLength           int
	EventsPort              uint16
	FrontendURL             string
	PingPeriod              base.Duration
	Port                    uint16
	Proxied                 bool
	ScoreboardUpdateSecret  string
	ScoreboardUpdateTimeout base.Duration
	TLS                     TLSConfig // only used if Proxied == false
	WriteDeadline           base.Duration
}

// InputManagerConfig represents the configuration for the InputManager.
type InputManagerConfig struct {
	CacheSize base.Byte
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
}

// GraderEphemeralConfig represents the configuration for the Grader web interface.
type GraderEphemeralConfig struct {
	EphemeralSizeLimit   base.Byte
	CaseTimeLimit        base.Duration
	OverallWallTimeLimit base.Duration
	MemoryLimit          base.Byte
	PingPeriod           base.Duration
	Port                 uint16
	Proxied              bool
	TLS                  TLSConfig // only used if Proxied == false
	WriteDeadline        base.Duration
}

// GraderCIConfig represents the configuration for the Grader CI.
type GraderCIConfig struct {
	CISizeLimit base.Byte
}

// GraderConfig represents the configuration for the Grader.
type GraderConfig struct {
	ChannelLength   int
	Port            uint16
	RuntimePath     string
	MaxGradeRetries int
	BroadcasterURL  string
	V1              V1Config
	Ephemeral       GraderEphemeralConfig
	CI              GraderCIConfig
	WriteGradeFiles bool // TODO(lhchavez): Remove once migration is done.
}

// TLSConfig represents the configuration for TLS.
type TLSConfig struct {
	CertFile string
	KeyFile  string
}

// RunnerConfig represents the configuration for the Runner.
type RunnerConfig struct {
	GraderURL          string
	RuntimePath        string
	CompileTimeLimit   base.Duration
	CompileOutputLimit base.Byte
	OmegajailRoot      string
	PreserveFiles      bool
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
	JSON  bool
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
		PingPeriod:              base.Duration(time.Duration(30) * time.Second),
		Port:                    32672,
		Proxied:                 true,
		ScoreboardUpdateSecret:  "secret",
		ScoreboardUpdateTimeout: base.Duration(time.Duration(10) * time.Second),
		TLS: TLSConfig{
			CertFile: "/etc/omegaup/broadcaster/certificate.pem",
			KeyFile:  "/etc/omegaup/broadcaster/key.pem",
		},
		WriteDeadline: base.Duration(time.Duration(5) * time.Second),
	},
	Db: DbConfig{
		Driver:         "sqlite3",
		DataSourceName: "./omegaup.db",
	},
	InputManager: InputManagerConfig{
		CacheSize: base.Gibibyte,
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
		},
		Ephemeral: GraderEphemeralConfig{
			EphemeralSizeLimit:   base.Gibibyte,
			CaseTimeLimit:        base.Duration(time.Duration(10) * time.Second),
			OverallWallTimeLimit: base.Duration(time.Duration(10) * time.Second),
			MemoryLimit:          base.Gibibyte,
			Port:                 36663,
			PingPeriod:           base.Duration(time.Duration(30) * time.Second),
			Proxied:              true,
			TLS: TLSConfig{
				CertFile: "/etc/omegaup/grader/web-certificate.pem",
				KeyFile:  "/etc/omegaup/grader/web-key.pem",
			},
			WriteDeadline: base.Duration(time.Duration(5) * time.Second),
		},
		CI: GraderCIConfig{
			CISizeLimit: base.Byte(256) * base.Mebibyte,
		},
		WriteGradeFiles: true,
	},
	Runner: RunnerConfig{
		RuntimePath:        "/var/lib/omegaup/runner",
		GraderURL:          "https://omegaup.com:11302",
		CompileTimeLimit:   base.Duration(time.Duration(30) * time.Second),
		CompileOutputLimit: base.Byte(10) * base.Mebibyte,
		OmegajailRoot:      "/var/lib/omegajail",
		PreserveFiles:      false,
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
	Context         context.Context
	Config          Config
	Log             log15.Logger
	EventCollector  EventCollector
	EventFactory    *EventFactory
	Metrics         base.Metrics
	logBuffer       *bytes.Buffer
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
// creates a Logger. The role is just an arbitrary string that will be used to
// disambiguate process names in the tracing in case multiple roles run from
// the same host (e.g. grader and runner in the development VM).
func NewContext(config *Config, role string) (*Context, error) {
	var ctx = Context{
		Context: context.Background(),
		Config:  *config,
		Metrics: &base.NoOpMetrics{},
	}

	// Logging
	if config.Logging.File != "" {
		var err error
		ctx.Log, err = base.RotatingLog(
			ctx.Config.Logging.File,
			ctx.Config.Logging.Level,
			config.Logging.JSON,
		)
		if err != nil {
			return nil, err
		}
	} else if config.Logging.Level == "debug" {
		ctx.Log = base.StderrLog(config.Logging.JSON)
	} else {
		ctx.Log = log15.New()
		ctx.Log.SetHandler(base.ErrorCallerStackHandler(
			log15.LvlInfo,
			base.StderrHandler(config.Logging.JSON),
		))
	}

	// Tracing
	if ctx.Config.Tracing.Enabled {
		tracingFile, err := base.NewRotatingFile(
			ctx.Config.Tracing.File,
			0644,
			func(tracingFile *os.File, isEmpty bool) error {
				_, err := tracingFile.Write([]byte("[\n"))
				return err
			},
		)
		if err != nil {
			return nil, err
		}
		ctx.EventCollector = NewWriterEventCollector(tracingFile)
	} else {
		ctx.EventCollector = &NullEventCollector{}
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "main"
	}
	ctx.EventFactory = NewEventFactory(
		fmt.Sprintf("%s (%s)", hostname, role),
		"main",
	)
	ctx.EventFactory.Register(ctx.EventCollector)

	return &ctx, nil
}

// NewContextFromReader creates a new Context from the specified reader. This
// also creates a Logger.
func NewContextFromReader(reader io.Reader, role string) (*Context, error) {
	config, err := NewConfig(reader)
	if err != nil {
		return nil, err
	}
	return NewContext(config, role)
}

// Close releases all resources owned by the context.
func (ctx *Context) Close() {
	ctx.EventCollector.Close()
	if closer, ok := ctx.Log.GetHandler().(io.Closer); ok {
		closer.Close()
	}
}

// DebugContext returns a new Context with an additional handler with a more
// verbose filter (using the Debug level) and a Buffer in which all logging
// statements will be (also) written to.
func (ctx *Context) DebugContext(logCtx ...interface{}) *Context {
	var buffer bytes.Buffer
	childContext := &Context{
		Context:         ctx.Context,
		Config:          ctx.Config,
		Log:             ctx.Log.New(logCtx...),
		logBuffer:       &buffer,
		memoryCollector: NewMemoryEventCollector(),
		EventFactory:    ctx.EventFactory,
		Metrics:         ctx.Metrics,
	}
	childContext.EventCollector = NewMultiEventCollector(
		childContext.memoryCollector,
		ctx.EventCollector,
	)
	childContext.EventFactory.Register(childContext.memoryCollector)
	childContext.Log.SetHandler(log15.MultiHandler(
		base.ErrorCallerStackHandler(
			log15.LvlDebug,
			log15.StreamHandler(&buffer, log15.LogfmtFormat()),
		),
		ctx.Log.GetHandler(),
	))
	return childContext
}

// AppendLogSection adds a complete section of logs to the log buffer. This
// typcally comes from a client.
func (ctx *Context) AppendLogSection(sectionName string, contents []byte) {
	fmt.Fprintf(ctx.logBuffer, "================  %s  ================\n", sectionName)
	ctx.logBuffer.Write(contents)
	fmt.Fprintf(ctx.logBuffer, "================ /%s  ================\n", sectionName)
}

// LogBuffer returns the contents of the logging buffer for this context.
func (ctx *Context) LogBuffer() []byte {
	if ctx.logBuffer == nil {
		return nil
	}
	return ctx.logBuffer.Bytes()
}

// TraceBuffer returns a JSON representation of the Trace Event stream for this
// Context.
func (ctx *Context) TraceBuffer() []byte {
	if ctx.memoryCollector == nil {
		return nil
	}
	data, err := json.Marshal(ctx.memoryCollector)
	if err != nil {
		return []byte(err.Error())
	}
	return data
}
