package common

import (
	"bytes"
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/inconshreveable/log15"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

var DefaultInputManager *InputManager

// Configuration
type GraderConfig struct {
	CacheSize     int64
	ChannelLength int
	Port          uint16
	RuntimePath   string
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

type LoggingConfig struct {
	File  string
	Level string
}

type Config struct {
	Grader  GraderConfig
	Db      DbConfig
	Logging LoggingConfig
	Runner  RunnerConfig
	TLS     TLSConfig
}

var defaultConfig = Config{
	Db: DbConfig{
		Driver:         "sqlite3",
		DataSourceName: "./omegaup.db",
	},
	Logging: LoggingConfig{
		File:  "/var/log/omegaup/service.log",
		Level: "info",
	},
	Grader: GraderConfig{
		CacheSize:     1024 * 1024 * 1024, // 1 GiB
		ChannelLength: 1024,
		Port:          11302,
		RuntimePath:   "/var/lib/omegaup/",
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
	Config  Config
	DB      *sql.DB
	Log     log15.Logger
	Buffer  *bytes.Buffer
	handler log15.Handler
}

// NewContext creates a new Context from the specified path. This also creates
// a Logger.
func NewContext(configPath string) (*Context, error) {
	var context = Context{
		Config: defaultConfig,
	}

	// Read basic config
	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
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
		context.handler, err = log15.FileHandler(context.Config.Logging.File,
			log15.LogfmtFormat())
		if err != nil {
			return nil, err
		}
	}
	level, err := log15.LvlFromString(context.Config.Logging.Level)
	if err != nil {
		return nil, err
	}
	context.handler = log15.LvlFilterHandler(level, context.handler)
	context.Log.SetHandler(context.handler)

	// Database
	context.DB, err = sql.Open(context.Config.Db.Driver,
		context.Config.Db.DataSourceName)
	if err != nil {
		return nil, err
	}
	if err := context.DB.Ping(); err != nil {
		return nil, err
	}

	return &context, nil
}

// Close releases all resources owned by the context.
func (context *Context) Close() {
	context.DB.Close()
}

// DebugContext returns a new Context with an additional handler with a more
// verbose filter (using the Debug level) and a Buffer in which all logging
// statements will be (also) written to.
func (parent *Context) DebugContext() *Context {
	var buffer bytes.Buffer
	var context = Context{
		Config: parent.Config,
		Log:    parent.Log.New(),
		Buffer: &buffer,
		handler: log15.MultiHandler(
			log15.StreamHandler(&buffer, log15.LogfmtFormat()),
			parent.handler,
		),
	}
	context.Log.SetHandler(context.handler)
	return &context
}
