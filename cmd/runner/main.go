package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"github.com/lhchavez/quark/common"
	"github.com/lhchavez/quark/runner"
	"golang.org/x/net/http2"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	insecure   = flag.Bool("insecure", false, "Do not use TLS")
	configPath = flag.String("config", "/etc/omegaup/runner/config.json",
		"Runner configuration file")
	globalContext atomic.Value
	ioLock        sync.Mutex
	inputManager  *common.InputManager
	minijail      runner.MinijailSandbox
)

func loadContext() error {
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, err := common.NewContext(f)
	if err != nil {
		return err
	}
	globalContext.Store(ctx)
	return nil
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	flag.Parse()

	if err := loadContext(); err != nil {
		panic(err)
	}

	ctx := globalContext.Load().(*common.Context)
	expvar.Publish("config", &globalContext.Load().(*common.Context).Config)
	inputManager = common.NewInputManager(ctx)
	inputPath := path.Join(ctx.Config.Runner.RuntimePath, "input")
	go inputManager.PreloadInputs(
		inputPath,
		runner.NewRunnerCachedInputFactory(inputPath),
		&ioLock,
	)
	var client *http.Client
	if *insecure {
		client = http.DefaultClient
	} else {
		cert, err := ioutil.ReadFile(ctx.Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(
			ctx.Config.TLS.CertFile,
			ctx.Config.TLS.KeyFile,
		)
		if err != nil {
			panic(err)
		}
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{keyPair},
				RootCAs:      certPool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			},
			ResponseHeaderTimeout: time.Duration(5 * time.Minute),
			// Workaround for https://github.com/golang/go/issues/14391
			ExpectContinueTimeout: 0,
		}
		if err := http2.ConfigureTransport(transport); err != nil {
			panic(err)
		}
		client = &http.Client{Transport: transport}
	}

	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}

	ctx.Log.Info("omegaUp runner ready to serve")

	var sleepTime float32 = 1
	go func() {
		ctx.Log.Error("http listen and serve", "err", http.ListenAndServe(":6060", nil))
	}()

	for {
		if err := processRun(ctx, client, baseURL); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Timeouts are expected. Just retry.
				sleepTime = 1
				continue
			}
			ctx.Log.Error("error grading run", "err", err)
			// Randomized exponential backoff.
			time.Sleep(time.Duration(rand.Float32()*sleepTime) * time.Second)
			if sleepTime < 64 {
				sleepTime *= 2
			}
		} else {
			sleepTime = 1
		}
	}
}

// ChannelBuffer is a buffer that implementats io.Reader, io.Writer, and
// io.WriterTo. Write() stores the incoming slices in a []byte channel, which
// are then consumed when either Read() or WriteTo() are called.
type ChannelBuffer struct {
	chunks       chan []byte
	currentChunk []byte
}

func NewChannelBuffer() *ChannelBuffer {
	return &ChannelBuffer{
		chunks: make(chan []byte, 0),
	}
}

func (cb *ChannelBuffer) CloseChannel() {
	close(cb.chunks)
}

func (cb *ChannelBuffer) Write(buf []byte) (int, error) {
	cb.chunks <- buf
	return len(buf), nil
}

func (cb *ChannelBuffer) WriteTo(w io.Writer) (int64, error) {
	totalWritten := int64(0)
	for {
		if cb.currentChunk == nil {
			c, ok := <-cb.chunks
			if !ok {
				return totalWritten, nil
			}
			cb.currentChunk = c
		}

		written, err := w.Write(cb.currentChunk)
		totalWritten += int64(written)
		if err != nil {
			return totalWritten, err
		}
		if written == len(cb.currentChunk) {
			cb.currentChunk = nil
		} else if written > 0 {
			cb.currentChunk = cb.currentChunk[written:]
		}
	}
}

func (cb *ChannelBuffer) Close() error {
	// If there are any chunks remaining, they should be drained before
	// returning.  The other goroutine should eventually close the channel.
	for _ = range cb.chunks {
	}
	return nil
}

func (cb *ChannelBuffer) Read(buf []byte) (int, error) {
	if cb.currentChunk == nil {
		c, ok := <-cb.chunks
		if !ok {
			return 0, io.EOF
		}
		cb.currentChunk = c
	}

	written := copy(buf, cb.currentChunk)
	if written == len(cb.currentChunk) {
		cb.currentChunk = nil
	} else {
		cb.currentChunk = cb.currentChunk[written:]
	}

	return written, nil
}

func processRun(
	parentCtx *common.Context,
	client *http.Client,
	baseURL *url.URL,
) error {
	requestURL, err := baseURL.Parse("run/request/")
	if err != nil {
		panic(err)
	}
	resp, err := client.Get(requestURL.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	ctx := parentCtx.DebugContext()
	syncID, err := strconv.ParseUint(resp.Header.Get("Sync-ID"), 10, 64)
	if err != nil {
		return err
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewReceiverClockSyncEvent(syncID))

	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		return err
	}
	uploadURL, err := baseURL.Parse(fmt.Sprintf("run/%d/results/", run.AttemptID))
	if err != nil {
		return err
	}

	finished := make(chan error, 1)

	if err = gradeAndUploadResults(
		ctx,
		client,
		uploadURL.String(),
		&run,
		finished,
	); err != nil {
		return err
	}

	return <-finished
}

func gradeAndUploadResults(
	ctx *common.Context,
	client *http.Client,
	uploadURL string,
	run *common.Run,
	finished chan<- error,
) error {
	requestBody := NewChannelBuffer()
	defer requestBody.CloseChannel()
	multipartWriter := multipart.NewWriter(requestBody)
	defer multipartWriter.Close()
	go func() {
		defer requestBody.Close()
		req, err := http.NewRequest("POST", uploadURL, requestBody)
		if err != nil {
			finished <- err
			close(finished)
			return
		}
		req.Header.Add("Content-Type", multipartWriter.FormDataContentType())
		response, err := client.Do(req)
		if err != nil {
			finished <- err
			close(finished)
			return
		}
		response.Body.Close()
		finished <- nil
		close(finished)
	}()

	result, err := gradeRun(ctx, client, run, multipartWriter)
	if err != nil {
		// Still try to send the details
		ctx.Log.Error("Error grading run", "err", err)
		result = &runner.RunResult{
			Verdict:  "JE",
			MaxScore: run.MaxScore,
		}
	}

	// Send results.
	resultWriter, err := multipartWriter.CreateFormFile("file", "details.json")
	if err != nil {
		ctx.Log.Error("Error sending details.json", "err", err)
		return err
	}
	encoder := json.NewEncoder(resultWriter)
	if err := encoder.Encode(result); err != nil {
		ctx.Log.Error("Error encoding details.json", "err", err)
		return err
	}

	// Send uncompressed logs.
	logsBuffer := ctx.LogBuffer()
	if logsBuffer != nil {
		logsWriter, err := multipartWriter.CreateFormFile("file", "logs.txt")
		if err != nil {
			ctx.Log.Error("Error creating logs.txt", "err", err)
			return err
		}
		if _, err = logsWriter.Write(logsBuffer); err != nil {
			ctx.Log.Error("Error sending logs.txt", "err", err)
		}
	}

	// Send uncompressed tracing data.
	traceBuffer := ctx.TraceBuffer()
	if traceBuffer != nil {
		tracingWriter, err := multipartWriter.CreateFormFile("file", "tracing.json")
		if err != nil {
			ctx.Log.Error("Error creating tracing.json", "err", err)
			return err
		}
		if _, err = tracingWriter.Write(traceBuffer); err != nil {
			ctx.Log.Error("Error sending tracing.json", "err", err)
			return err
		}
	}

	return nil
}

func gradeRun(
	ctx *common.Context,
	client *http.Client,
	run *common.Run,
	multipartWriter *multipart.Writer,
) (*runner.RunResult, error) {
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent(
		"grade",
		common.EventBegin,
		common.Arg{"id", run.AttemptID},
		common.Arg{"input", run.InputHash},
		common.Arg{"language", run.Language},
	))
	defer func() {
		ctx.EventCollector.Add(ctx.EventFactory.NewEvent(
			"grade",
			common.EventEnd,
		))
	}()

	// Make sure no other I/O is being made while we grade this run.
	ioLockEvent := ctx.EventFactory.NewCompleteEvent("I/O lock")
	ioLock.Lock()
	defer ioLock.Unlock()
	ctx.EventCollector.Add(ioLockEvent)

	inputEvent := ctx.EventFactory.NewCompleteEvent("input")
	input, err := inputManager.Add(
		run.InputHash,
		runner.NewRunnerInputFactory(client, &ctx.Config),
	)
	if err != nil {
		return nil, err
	}
	defer input.Release(input)
	ctx.EventCollector.Add(inputEvent)

	// Send the header as soon as possible to avoid a timeout.
	filesWriter, err := multipartWriter.CreateFormFile("file", "files.zip")
	if err != nil {
		return nil, err
	}

	return runner.Grade(ctx, filesWriter, run, input, &minijail)
}
