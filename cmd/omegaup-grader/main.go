package main

//go:generate ${GOPATH}/bin/go-bindata -nomemcopy data/dist/...

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"expvar"
	"flag"
	"fmt"
	"github.com/coreos/go-systemd/daemon"
	_ "github.com/go-sql-driver/mysql"
	git "github.com/lhchavez/git2go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	version    = flag.Bool("version", false, "Print the version and exit")
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	skipAssets = flag.Bool("skip-assets", false, "Do not use pre-packaged assets")
	configPath = flag.String(
		"config",
		"/etc/omegaup/grader/config.json",
		"Grader configuration file",
	)
	globalContext atomic.Value
	server        *http.Server

	// ProgramVersion is the version of the code from which the binary was built from.
	ProgramVersion string
)

type processRunStatus struct {
	status int
	retry  bool
}

// A ResponseStruct represents the result of a run request.
type ResponseStruct struct {
	Results  string
	Logs     string
	FilesZip string
	Tracing  string
}

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, err := grader.NewContext(f)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func graderContext() *grader.Context {
	return globalContext.Load().(*grader.Context)
}

func peerName(r *http.Request, insecure bool) string {
	if insecure {
		return r.RemoteAddr
	}
	return r.TLS.PeerCertificates[0].Subject.CommonName
}

func readGzippedFile(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, gz); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func readBase64File(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var buf bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &buf)
	if err != nil {
		return "", err
	}

	if _, err := io.Copy(enc, f); err != nil {
		return "", err
	}
	enc.Close()
	return buf.String(), nil
}

type wrappedFileSystem struct {
	fileSystem http.FileSystem
}

func (fs *wrappedFileSystem) Open(name string) (http.File, error) {
	if *skipAssets {
		path := "/data" + filepath.Clean(filepath.Join("/", name))
		return os.Open(path)
	}
	if file, err := fs.fileSystem.Open(name); err == nil {
		return file, nil
	}
	return nil, os.ErrNotExist
}

func queueEventsProcessor(events <-chan *grader.QueueEvent) {
	ctx := graderContext()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}

			switch event.Type {
			case grader.QueueEventTypeManagerAdded:
				ctx.Metrics.GaugeAdd("grader_queue_total_length", 1)
			case grader.QueueEventTypeManagerRemoved:
				ctx.Metrics.GaugeAdd("grader_queue_total_length", -1)
				ctx.Metrics.SummaryObserve("grader_queue_delay_seconds", event.Delta.Seconds())
			case grader.QueueEventTypeQueueRemoved:
				switch event.Priority {
				case grader.QueuePriorityEphemeral:
					ctx.Metrics.SummaryObserve("grader_queue_ephemeral_delay_seconds", event.Delta.Seconds())
				case grader.QueuePriorityLow:
					ctx.Metrics.SummaryObserve("grader_queue_low_delay_seconds", event.Delta.Seconds())
				case grader.QueuePriorityNormal:
					ctx.Metrics.SummaryObserve("grader_queue_normal_delay_seconds", event.Delta.Seconds())
				case grader.QueuePriorityHigh:
					ctx.Metrics.SummaryObserve("grader_queue_high_delay_seconds", event.Delta.Seconds())
				}
			case grader.QueueEventTypeRetried:
				ctx.Metrics.GaugeAdd("grader_runs_retry", 1)
			case grader.QueueEventTypeAbandoned:
				ctx.Metrics.GaugeAdd("grader_runs_abandoned", 1)
			}
		}
	}
}

func main() {
	defer git.Shutdown()

	flag.Parse()

	if *version {
		fmt.Printf("omegaup-grader %s\n", ProgramVersion)
		return
	}

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := graderContext()
	expvar.Publish("config", &ctx.Config)

	expvar.Publish("codemanager", expvar.Func(func() interface{} {
		return graderContext().InputManager
	}))
	expvar.Publish("queues", expvar.Func(func() interface{} {
		return graderContext().QueueManager
	}))
	expvar.Publish("inflight_runs", expvar.Func(func() interface{} {
		return graderContext().InflightMonitor
	}))
	cachePath := path.Join(ctx.Config.Grader.RuntimePath, "cache")
	go ctx.InputManager.PreloadInputs(
		cachePath,
		grader.NewCachedInputFactory(cachePath),
		&sync.Mutex{},
	)

	// Database
	db, err := sql.Open(
		ctx.Config.Db.Driver,
		ctx.Config.Db.DataSourceName,
	)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}

	setupMetrics(ctx)
	var servers []*http.Server
	var wg sync.WaitGroup
	{
		mux := http.NewServeMux()
		registerEphemeralHandlers(ctx, mux)
		servers = append(
			servers,
			common.RunServer(
				&ctx.Config.Grader.Ephemeral.TLS,
				mux,
				&wg,
				fmt.Sprintf(":%d", ctx.Config.Grader.Ephemeral.Port),
				ctx.Config.Grader.Ephemeral.Proxied,
			),
		)
	}

	queueEventsChan := make(chan *grader.QueueEvent, 1)
	graderContext().QueueManager.AddEventListener(queueEventsChan)
	go queueEventsProcessor(queueEventsChan)

	mux := http.DefaultServeMux
	if ctx.Config.Grader.V1.Enabled {
		registerV1CompatHandlers(mux, db)
		servers = append(
			servers,
			common.RunServer(
				&ctx.Config.TLS,
				mux,
				&wg,
				fmt.Sprintf(":%d", ctx.Config.Grader.V1.Port),
				*insecure,
			),
		)
		mux = http.NewServeMux()
	}

	registerHandlers(ctx, mux, db, *insecure)
	servers = append(
		servers,
		common.RunServer(
			&ctx.Config.TLS,
			mux,
			&wg,
			fmt.Sprintf(":%d", ctx.Config.Grader.Port),
			*insecure,
		),
	)

	ctx.Log.Info("omegaUp grader started")
	daemon.SdNotify(false, "READY=1")

	<-stopChan

	ctx.Log.Info("Shutting down server...")
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for _, server := range servers {
		server.Shutdown(cancelCtx)
	}

	cancel()
	wg.Wait()

	ctx.Log.Info("Server gracefully stopped.")
}
