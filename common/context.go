package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/omegaup/go-base/logging/log15"
	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/go-base/v3/logging"
	"github.com/omegaup/go-base/v3/tracing"
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
	ChannelLength          int
	Port                   uint16
	RuntimePath            string
	MaxGradeRetries        int
	BroadcasterURL         string
	GitserverURL           string
	GitserverAuthorization string
	V1                     V1Config
	Ephemeral              GraderEphemeralConfig
	CI                     GraderCIConfig
	UseS3                  bool
}

// TLSConfig represents the configuration for TLS.
type TLSConfig struct {
	CertFile string
	KeyFile  string
}

// RunnerConfig represents the configuration for the Runner.
type RunnerConfig struct {
	Hostname           string
	PublicIP           string
	GraderURL          string
	RuntimePath        string
	CompileTimeLimit   base.Duration
	CompileOutputLimit base.Byte
	HardMemoryLimit    base.Byte
	OverallOutputLimit base.Byte
	OmegajailRoot      string
	PreserveFiles      bool
}

// DbConfig represents the configuration for the database.
type DbConfig struct {
	Driver         string
	DataSourceName string
}

// LoggingConfig represents the configuration for logging.
type LoggingConfig struct {
	Level string
	JSON  bool
}

// NewRelicConfig represents the configuration for NewRelic.
type NewRelicConfig struct {
	AppName string
	License string
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
	NewRelic     NewRelicConfig
	Metrics      MetricsConfig
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
		Level: "info",
	},
	NewRelic: NewRelicConfig{
		AppName: "quark.omegaup.com",
	},
	Metrics: MetricsConfig{
		Port: 6060,
	},
	Grader: GraderConfig{
		BroadcasterURL:         "https://omegaup.com:32672/broadcast/",
		GitserverURL:           "https://gitserver.omegaup.com/",
		GitserverAuthorization: "",
		ChannelLength:          1024,
		Port:                   11302,
		RuntimePath:            "/var/lib/omegaup/",
		MaxGradeRetries:        3,
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
		UseS3: false,
	},
	Runner: RunnerConfig{
		RuntimePath:        "/var/lib/omegaup/runner",
		GraderURL:          "https://omegaup.com:11302",
		CompileTimeLimit:   base.Duration(time.Duration(30) * time.Second),
		CompileOutputLimit: base.Byte(10) * base.Mebibyte,
		HardMemoryLimit:    base.Byte(640) * base.Mebibyte,
		OverallOutputLimit: base.Byte(100) * base.Mebibyte,
		OmegajailRoot:      "/var/lib/omegajail",
		PreserveFiles:      false,
	},
	TLS: TLSConfig{
		CertFile: "/etc/omegaup/grader/certificate.pem",
		KeyFile:  "/etc/omegaup/grader/key.pem",
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
	Context     context.Context
	Config      Config
	Log         logging.Logger
	Metrics     base.Metrics
	Tracing     tracing.Provider
	Transaction tracing.Transaction
	logBuffer   *bytes.Buffer
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
	var ctx = Context{
		Context:     context.Background(),
		Config:      *config,
		Metrics:     &base.NoOpMetrics{},
		Tracing:     tracing.NewNoOpProvider(),
		Transaction: tracing.NewNoOpTransaction(),
	}

	// Logging
	var err error
	ctx.Log, err = log15.New(ctx.Config.Logging.Level, config.Logging.JSON)
	if err != nil {
		return nil, err
	}

	return &ctx, nil
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
func (ctx *Context) Close() {
	if closer, ok := ctx.Log.(io.Closer); ok {
		closer.Close()
	}
}

// Wrap returns a new Context with the applied context.
func (ctx *Context) Wrap(c context.Context) *Context {
	wrapped := *ctx
	wrapped.Context = c
	wrapped.Log = wrapped.Log.NewContext(wrapped.Context)
	return &wrapped
}

// DebugContext returns a new Context with an additional handler with a more
// verbose filter (using the Debug level) and a Buffer in which all logging
// statements will be (also) written to.
func (ctx *Context) DebugContext(logCtx map[string]any) *Context {
	var buffer bytes.Buffer
	childContext := &Context{
		Context: ctx.Context,
		Config:  ctx.Config,
		Log: logging.NewMultiLogger(
			ctx.Log.New(logCtx),
			logging.NewInMemoryLogfmtLogger(&buffer),
		),
		logBuffer:   &buffer,
		Metrics:     ctx.Metrics,
		Tracing:     ctx.Tracing,
		Transaction: ctx.Transaction,
	}
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
