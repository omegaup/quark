package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/omegaup/go-base/v3/tracing"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/runner"

	"github.com/pkg/errors"
)

func benchmarkLoop(ctx *common.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		results, err := runner.RunHostBenchmark(
			ctx,
			inputManager,
			sandbox,
			&ioLock,
		)
		if err != nil {
			ctx.Log.Error(
				"Failed to run benchmark",
				map[string]interface{}{
					"err": err,
				},
			)
		} else {
			ctx.Log.Info(
				"Benchmark successful",
				map[string]interface{}{
					"results": results,
				},
			)
		}
		gaugesUpdate(results)

		select {
		case <-ctx.Context.Done():
			return
		case <-time.After(time.Duration(1) * time.Minute):
			// continue with the loop.
		}
	}
}

func runnerLoop(ctx *common.Context, wg *sync.WaitGroup, client *http.Client, baseURL *url.URL) {
	wg.Add(1)
	defer wg.Done()
	var sleepTime float32 = 1

	for {
		if err := processRun(ctx, client, baseURL); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Timeouts are expected. Just retry.
				sleepTime = 1
				continue
			}
			ctx.Log.Error(
				"error grading run",
				map[string]interface{}{
					"err": err,
				},
			)
			// Randomized exponential backoff.
			select {
			case <-ctx.Context.Done():
				return
			case <-time.After(time.Duration(rand.Float32()*sleepTime) * time.Second):
				// continue with the loop.
			}
			if sleepTime < 64 {
				sleepTime *= 2
			}
		} else {
			sleepTime = 1
		}
	}
}

// channelBuffer is a buffer that implements io.Reader, io.Writer, and
// io.WriterTo. Write() stores the incoming slices in a []byte channel, which
// are then consumed when either Read() or WriteTo() are called.
type channelBuffer struct {
	chunks       chan []byte
	currentChunk []byte
}

func newChannelBuffer() *channelBuffer {
	return &channelBuffer{
		chunks: make(chan []byte, 0),
	}
}

func (cb *channelBuffer) closeChannel() {
	close(cb.chunks)
}

func (cb *channelBuffer) Write(buf []byte) (int, error) {
	// Copy the buffer since we cannot guarantee that the caller will not write
	// to it again while we are waiting for the other end to read from it.
	innerbuf := make([]byte, len(buf))
	copy(innerbuf, buf)

	cb.chunks <- innerbuf
	return len(innerbuf), nil
}

func (cb *channelBuffer) WriteTo(w io.Writer) (int64, error) {
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

func (cb *channelBuffer) Close() error {
	// If there are any chunks remaining, they should be drained before
	// returning.  The other goroutine should eventually close the channel.
	for range cb.chunks {
	}
	return nil
}

func (cb *channelBuffer) Read(buf []byte) (int, error) {
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
	req, err := http.NewRequestWithContext(parentCtx.Context, "GET", requestURL.String(), nil)
	if err != nil {
		return err
	}
	if parentCtx.Config.Runner.Hostname != "" {
		req.Header.Add("OmegaUp-Runner-Name", parentCtx.Config.Runner.Hostname)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errors.Errorf("non-2xx error code returned: %d", resp.StatusCode)
	}

	ctx := parentCtx.DebugContext(nil)
	ctx.Transaction = ctx.Tracing.StartTransaction("run")
	ctx.Transaction.AcceptDistributedTraceHeaders(tracing.TransportQueue, resp.Header)
	defer ctx.Transaction.End()

	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		return errors.Wrap(err, "failed to parse the run request body")
	}
	uploadURL, err := baseURL.Parse(fmt.Sprintf("run/%d/results/", run.AttemptID))
	if err != nil {
		return errors.Wrap(err, "failed to create the result upload URL")
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
	requestBody := newChannelBuffer()
	defer requestBody.closeChannel()
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
		if ctx.Config.Runner.Hostname != "" {
			req.Header.Add("OmegaUp-Runner-Name", ctx.Config.Runner.Hostname)
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

	filesWriter := newFilesZipWriter(multipartWriter)
	result, err := gradeRun(ctx, client, run, filesWriter)
	filesWriter.Close()
	if err != nil {
		// Still try to send the details
		ctx.Log.Error(
			"Error grading run",
			map[string]interface{}{
				"err": err,
			},
		)
		result = runner.NewRunResult("JE", run.MaxScore)
	}

	if *noop {
		runner.NoopSandboxFixupResult(result)
	}

	// Send results.
	resultWriter, err := multipartWriter.CreateFormFile("file", "details.json")
	if err != nil {
		ctx.Log.Error(
			"Error sending details.json",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}
	encoder := json.NewEncoder(resultWriter)
	if err := encoder.Encode(result); err != nil {
		ctx.Log.Error(
			"Error encoding details.json",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}

	// Send uncompressed logs.
	logsBuffer := ctx.LogBuffer()
	if logsBuffer != nil {
		logsWriter, err := multipartWriter.CreateFormFile("file", "logs.txt")
		if err != nil {
			ctx.Log.Error(
				"Error creating logs.txt",
				map[string]interface{}{
					"err": err,
				},
			)
			return err
		}
		if _, err = logsWriter.Write(logsBuffer); err != nil {
			ctx.Log.Error(
				"Error sending logs.txt",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	}

	return nil
}

// filesZipWriter is an io.WriteCloser backed by a multipart.Writer that
// creates files called `.keepalive` every 15 seconds until the first real
// write is made. This allows the connection to avoid timing out due to nothing
// being sent for 60s.
type filesZipWriter struct {
	multipartWriter *multipart.Writer
	writeReadyChan  chan<- struct{}
	tickerDoneChan  <-chan struct{}
	once            sync.Once

	w    io.Writer
	wErr error
}

var _ io.WriteCloser = (*filesZipWriter)(nil)

func newFilesZipWriter(multipartWriter *multipart.Writer) *filesZipWriter {
	writeReadyChan := make(chan struct{})
	tickerDoneChan := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Second)
		for {
			select {
			case <-tick.C:
				multipartWriter.CreateFormFile("file", ".keepalive")
			case <-writeReadyChan:
				tick.Stop()
				close(tickerDoneChan)
				return
			}
		}
	}()
	return &filesZipWriter{
		multipartWriter: multipartWriter,
		writeReadyChan:  writeReadyChan,
		tickerDoneChan:  tickerDoneChan,
	}
}

// ready marks the writer as ready to start writing to the files.zip file. This
// will wait for any outstanding write to a `.keepalive` file, and stop trying
// to create such files.
func (w *filesZipWriter) ready() {
	w.once.Do(func() {
		close(w.writeReadyChan)
		<-w.tickerDoneChan

		w.w, w.wErr = w.multipartWriter.CreateFormFile("file", "files.zip")
	})
}

func (w *filesZipWriter) Write(b []byte) (int, error) {
	w.ready()
	if w.wErr != nil {
		return 0, w.wErr
	}
	return w.w.Write(b)
}

func (w *filesZipWriter) Close() error {
	w.ready()
	return w.wErr
}

func gradeRun(
	ctx *common.Context,
	client *http.Client,
	run *common.Run,
	filesWriter io.Writer,
) (*runner.RunResult, error) {
	defer ctx.Transaction.StartSegment("grade").End()

	// Make sure no other I/O is being made while we grade this run.
	ioLockSegment := ctx.Transaction.StartSegment("I/O lock")
	ioLock.Lock()
	defer ioLock.Unlock()
	ioLockSegment.End()

	inputSegment := ctx.Transaction.StartSegment("input")
	baseURL, err := url.Parse(ctx.Config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	inputRef, err := inputManager.Add(
		run.InputHash,
		runner.NewInputFactory(client, &ctx.Config, baseURL, run.ProblemName),
	)
	if err != nil {
		return nil, err
	}
	defer inputRef.Release()
	inputSegment.End()

	return runner.Grade(ctx, filesWriter, run, inputRef.Input, sandbox)
}
