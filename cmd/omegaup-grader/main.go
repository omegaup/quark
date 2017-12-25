package main

//go:generate go-bindata -nomemcopy data/dist/...

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/base64"
	"expvar"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/grader"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	skipAssets = flag.Bool("skip-assets", false, "Do not use pre-packaged assets")
	configPath = flag.String(
		"config",
		"/etc/omegaup/grader/config.json",
		"Grader configuration file",
	)
	globalContext atomic.Value
	server        *http.Server
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

func context() *grader.Context {
	return globalContext.Load().(*grader.Context)
}

func peerName(r *http.Request) string {
	if *insecure {
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

func main() {
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := context()
	expvar.Publish("config", &ctx.Config)

	expvar.Publish("codemanager", expvar.Func(func() interface{} {
		return context().InputManager
	}))
	expvar.Publish("queues", expvar.Func(func() interface{} {
		return context().QueueManager
	}))
	expvar.Publish("inflight_runs", expvar.Func(func() interface{} {
		return context().InflightMonitor
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
	ctx.Log.Info("omegaUp grader started")
	{
		mux := http.NewServeMux()
		registerEphemeralHandlers(mux)
		go common.RunServer(
			&ctx.Config.Grader.Ephemeral.TLS,
			mux,
			fmt.Sprintf(":%d", ctx.Config.Grader.Ephemeral.Port),
			ctx.Config.Grader.Ephemeral.Proxied,
		)
	}

	mux := http.DefaultServeMux
	if ctx.Config.Grader.V1.Enabled {
		registerV1CompatHandlers(mux, db)
		go common.RunServer(
			&ctx.Config.TLS,
			mux,
			fmt.Sprintf(":%d", ctx.Config.Grader.V1.Port),
			*insecure,
		)
		mux = http.NewServeMux()
	}

	registerHandlers(mux, db)
	common.RunServer(
		&ctx.Config.TLS,
		mux,
		fmt.Sprintf(":%d", ctx.Config.Grader.Port),
		*insecure,
	)
}
