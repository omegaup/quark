package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inconshreveable/log15"
	base "github.com/omegaup/go-base"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Duration is identical to time.Duration, except it can implements the
// json.Marshaler interface with time.Duration.String() and
// time.Duration.ParseDuration().
type Duration time.Duration

// String returns a string representing the duration in the form "72h3m0.5s".
// Leading zero units are omitted. As a special case, durations less than one
// second format use a smaller unit (milli-, micro-, or nanoseconds) to ensure
// that the leading digit is non-zero. The zero duration formats as 0s.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// MarshalJSON implements the json.Marshaler interface. The duration is a quoted
// string in RFC 3339 format, with sub-second precision added if present.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", d.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface. The duration is
// expected to be a quoted string that time.ParseDuration() can understand.
func (d *Duration) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	if unicode.IsDigit(rune(data[0])) {
		val, err := strconv.ParseFloat(string(data), 64)
		if err != nil {
			return nil
		}
		*d = Duration(time.Duration(val*1e9) * time.Nanosecond)
		return nil
	}
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("time: invalid duration " + string(data))
	}
	parsed, err := time.ParseDuration(string(data[1 : len(data)-1]))
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// Milliseconds returns the duration as a floating point number of milliseconds.
func (d Duration) Milliseconds() float64 {
	return float64(d) / float64(time.Millisecond)
}

// Seconds returns the duration as a floating point number of seconds.
func (d Duration) Seconds() float64 {
	return time.Duration(d).Seconds()
}

// MinDuration returns the smaller of x or y.
func MinDuration(x, y Duration) Duration {
	if x < y {
		return x
	}
	return y
}

// MaxDuration returns the larger of x or y.
func MaxDuration(x, y Duration) Duration {
	if x > y {
		return x
	}
	return y
}

// A Byte is a unit of digital information.
type Byte int64

const (
	// Kibibyte is 1024 Bytes.
	Kibibyte = Byte(1024)

	// Mebibyte is 1024 Kibibytes.
	Mebibyte = Byte(1024) * Kibibyte

	// Gibibyte is 1024 Mebibytes.
	Gibibyte = Byte(1024) * Mebibyte

	// Tebibyte is 1024 Gibibytes.
	Tebibyte = Byte(1024) * Gibibyte
)

// MinBytes returns the smaller of x or y.
func MinBytes(x, y Byte) Byte {
	if x < y {
		return x
	}
	return y
}

// MaxBytes returns the larger of x or y.
func MaxBytes(x, y Byte) Byte {
	if x > y {
		return x
	}
	return y
}

// Bytes returns the Byte as an integer number of bytes.
func (b Byte) Bytes() int64 {
	return int64(b)
}

// Kibibytes returns the Byte as an floating point number of Kibibytes.
func (b Byte) Kibibytes() float64 {
	return float64(b) / float64(Kibibyte)
}

// Mebibytes returns the Byte as an floating point number of Mebibytes.
func (b Byte) Mebibytes() float64 {
	return float64(b) / float64(Mebibyte)
}

// Gibibytes returns the Byte as an floating point number of Gibibytes.
func (b Byte) Gibibytes() float64 {
	return float64(b) / float64(Gibibyte)
}

// Tebibytes returns the Byte as an floating point number of Tebibytes.
func (b Byte) Tebibytes() float64 {
	return float64(b) / float64(Tebibyte)
}

// MarshalJSON implements the json.Marshaler interface. The result is an
// integer number of bytes.
func (b Byte) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", b.Bytes())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface. The result can be
// an integer number of bytes, or a quoted string that MarshalJSON() can
// understand.
func (b *Byte) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	if unicode.IsDigit(rune(data[0])) {
		val, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return nil
		}
		*b = Byte(val)
		return nil
	}
	unquoted := string(data)
	if len(unquoted) < 3 || unquoted[0] != '"' || unquoted[len(unquoted)-1] != '"' {
		return errors.New("byte: invalid byte " + unquoted)
	}
	unquoted = unquoted[1 : len(unquoted)-1]
	suffixPos := strings.IndexFunc(unquoted, func(c rune) bool {
		return c != '.' && !unicode.IsDigit(c)
	})
	if suffixPos == -1 {
		suffixPos = len(unquoted)
	}
	parsed, err := strconv.ParseFloat(unquoted[:suffixPos], 64)
	if err != nil {
		return err
	}
	unit := Byte(1)
	switch string(unquoted[suffixPos:]) {
	case "TiB":
		unit = Tebibyte
	case "GiB":
		unit = Gibibyte
	case "MiB":
		unit = Mebibyte
	case "KiB":
		unit = Kibibyte
	case "B":
	case "":
		unit = Byte(1)
	default:
		return errors.New("byte: invalid byte " + string(data))
	}
	*b = Byte(parsed * float64(unit))
	return nil
}

// BroadcasterConfig represents the configuration for the Broadcaster.
type BroadcasterConfig struct {
	ChannelLength           int
	EventsPort              uint16
	FrontendURL             string
	PingPeriod              Duration
	Port                    uint16
	Proxied                 bool
	ScoreboardUpdateSecret  string
	ScoreboardUpdateTimeout Duration
	TLS                     TLSConfig // only used if Proxied == false
	WriteDeadline           Duration
}

// InputManagerConfig represents the configuration for the InputManager.
type InputManagerConfig struct {
	CacheSize Byte
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

// GraderEphemeralConfig represents the configuration for the Grader web interface.
type GraderEphemeralConfig struct {
	EphemeralSizeLimit   Byte
	CaseTimeLimit        Duration
	OverallWallTimeLimit Duration
	MemoryLimit          Byte
	PingPeriod           Duration
	Port                 uint16
	Proxied              bool
	TLS                  TLSConfig // only used if Proxied == false
	WriteDeadline        Duration
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
	CompileTimeLimit    Duration
	CompileOutputLimit  Byte
	ClrVMEstimatedSize  Byte
	JavaVMEstimatedSize Byte
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
		PingPeriod:              Duration(time.Duration(30) * time.Second),
		Port:                    32672,
		Proxied:                 true,
		ScoreboardUpdateSecret:  "secret",
		ScoreboardUpdateTimeout: Duration(time.Duration(10) * time.Second),
		TLS: TLSConfig{
			CertFile: "/etc/omegaup/broadcaster/certificate.pem",
			KeyFile:  "/etc/omegaup/broadcaster/key.pem",
		},
		WriteDeadline: Duration(time.Duration(5) * time.Second),
	},
	Db: DbConfig{
		Driver:         "sqlite3",
		DataSourceName: "./omegaup.db",
	},
	InputManager: InputManagerConfig{
		CacheSize: Gibibyte,
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
		Ephemeral: GraderEphemeralConfig{
			EphemeralSizeLimit:   Gibibyte,
			CaseTimeLimit:        Duration(time.Duration(10) * time.Second),
			OverallWallTimeLimit: Duration(time.Duration(10) * time.Second),
			MemoryLimit:          Gibibyte,
			Port:                 36663,
			PingPeriod:           Duration(time.Duration(30) * time.Second),
			Proxied:              true,
			TLS: TLSConfig{
				CertFile: "/etc/omegaup/grader/web-certificate.pem",
				KeyFile:  "/etc/omegaup/grader/web-key.pem",
			},
			WriteDeadline: Duration(time.Duration(5) * time.Second),
		},
		WriteGradeFiles: true,
	},
	Runner: RunnerConfig{
		RuntimePath:         "/var/lib/omegaup/runner",
		GraderURL:           "https://omegaup.com:11302",
		CompileTimeLimit:    Duration(time.Duration(30) * time.Second),
		CompileOutputLimit:  Byte(10) * Mebibyte,
		ClrVMEstimatedSize:  Byte(20) * Mebibyte,
		JavaVMEstimatedSize: Byte(30) * Mebibyte,
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
	var context = Context{
		Config:  *config,
		Metrics: &NoOpMetrics{},
	}

	// Logging
	var err error
	if context.Log, err = base.RotatingLog(
		context.Config.Logging.File,
		context.Config.Logging.Level,
	); err != nil {
		return nil, err
	}

	// Tracing
	if context.Config.Tracing.Enabled {
		tracingFile, err := base.NewRotatingFile(
			context.Config.Tracing.File,
			0644,
			func(tracingFile *os.File, isEmpty bool) error {
				_, err := tracingFile.Write([]byte("[\n"))
				return err
			},
		)
		if err != nil {
			return nil, err
		}
		context.EventCollector = NewWriterEventCollector(tracingFile)
	} else {
		context.EventCollector = &NullEventCollector{}
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "main"
	}
	context.EventFactory = NewEventFactory(
		fmt.Sprintf("%s (%s)", hostname, role),
		"main",
	)
	context.EventFactory.Register(context.EventCollector)

	return &context, nil
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
func (context *Context) Close() {
	context.EventCollector.Close()
	if closer, ok := context.Log.GetHandler().(io.Closer); ok {
		closer.Close()
	}
}

// DebugContext returns a new Context with an additional handler with a more
// verbose filter (using the Debug level) and a Buffer in which all logging
// statements will be (also) written to.
func (context *Context) DebugContext(logCtx ...interface{}) *Context {
	var buffer bytes.Buffer
	childContext := &Context{
		Config:          context.Config,
		Log:             context.Log.New(logCtx...),
		logBuffer:       &buffer,
		memoryCollector: NewMemoryEventCollector(),
		EventFactory:    context.EventFactory,
		Metrics:         context.Metrics,
	}
	childContext.EventCollector = NewMultiEventCollector(
		childContext.memoryCollector,
		context.EventCollector,
	)
	childContext.EventFactory.Register(childContext.memoryCollector)
	childContext.Log.SetHandler(log15.MultiHandler(
		base.ErrorCallerStackHandler(
			log15.LvlDebug,
			log15.StreamHandler(&buffer, log15.LogfmtFormat()),
		),
		context.Log.GetHandler(),
	))
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

// ParseRational returns a rational that's within 1e-6 of the floating-point
// value that has been serialized as a string.
func ParseRational(str string) (*big.Rat, error) {
	floatVal, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, err
	}

	return FloatToRational(floatVal), nil
}

// FloatToRational returns a rational that's within 1e-6 of the floating-point
// value.
func FloatToRational(floatVal float64) *big.Rat {
	tolerance := 1e-6

	var chosenDenominator int64 = 1024
	for _, denominator := range []int64{1, 100, 1000, 720, 5040, 40320, 3628800} {
		scaledVal := floatVal * float64(denominator)
		if math.Abs(scaledVal-math.Round(scaledVal)) <= tolerance {
			chosenDenominator = denominator
			break
		}
	}

	return big.NewRat(
		int64(math.Round(floatVal*float64(chosenDenominator))),
		chosenDenominator,
	)
}

// RationalToFloat returns the closest float value to the given big.Rat.
func RationalToFloat(val *big.Rat) float64 {
	floatVal, _ := val.Float64()
	return floatVal
}

// RationalDiv implements division between two rationals.
func RationalDiv(num, denom *big.Rat) *big.Rat {
	val := new(big.Rat).SetFrac(denom.Denom(), denom.Num())
	return val.Mul(val, num)
}
