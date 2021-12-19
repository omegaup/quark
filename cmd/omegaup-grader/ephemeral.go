package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"math/big"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/go-base/v3/tracing"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
)

var (
	validEphemeralRunFilenames = map[string]struct{}{
		"details.json": {},
		"files.zip":    {},
		"logs.txt":     {},
		"request.json": {},
		"tracing.json": {},
	}
)

func saveEphemeralRunRequest(
	ctx *grader.Context,
	runInfo *grader.RunInfo,
	ephemeralRunRequest *grader.EphemeralRunRequest,
) error {
	var requestBuffer bytes.Buffer
	zw := gzip.NewWriter(&requestBuffer)
	err := json.NewEncoder(zw).Encode(ephemeralRunRequest)
	if err != nil {
		zw.Close()
		ctx.Log.Error(
			"Error marshaling json",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}
	err = zw.Close()
	if err != nil {
		ctx.Log.Error(
			"Error closing gzip stream",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}
	err = runInfo.Artifacts.Put(&ctx.Context, "request.json.gz", &requestBuffer)
	if err != nil {
		ctx.Log.Error(
			"Error writing request.json.gz",
			map[string]interface{}{
				"err": err,
			},
		)
		return err
	}
	return nil
}

type ephemeralRunHandler struct {
	ephemeralRunManager *grader.EphemeralRunManager
	ctx                 *grader.Context
}

func (h *ephemeralRunHandler) validateRequest(
	ctx *grader.Context,
	ephemeralRunRequest *grader.EphemeralRunRequest,
) error {
	if ephemeralRunRequest.Input.Limits == nil {
		return nil
	}
	// Silently apply some caps.
	ephemeralRunRequest.Input.Limits.TimeLimit = base.MinDuration(
		ctx.Config.Grader.Ephemeral.CaseTimeLimit,
		ephemeralRunRequest.Input.Limits.TimeLimit,
	)
	ephemeralRunRequest.Input.Limits.OverallWallTimeLimit = base.MinDuration(
		ctx.Config.Grader.Ephemeral.OverallWallTimeLimit,
		ephemeralRunRequest.Input.Limits.OverallWallTimeLimit,
	)
	ephemeralRunRequest.Input.Limits.MemoryLimit = base.MinBytes(
		ctx.Config.Grader.Ephemeral.MemoryLimit,
		ephemeralRunRequest.Input.Limits.MemoryLimit,
	)
	return nil
}

func (h *ephemeralRunHandler) addAndWaitForRun(
	ctx *grader.Context,
	w http.ResponseWriter,
	ephemeralRunRequest *grader.EphemeralRunRequest,
	runs *grader.Queue,
) error {
	ctx.Metrics.CounterAdd("grader_ephemeral_runs_total", 1)
	ctx.Log.Debug(
		"Adding new run",
		map[string]interface{}{
			"run": ephemeralRunRequest,
		},
	)
	if err := h.validateRequest(ctx, ephemeralRunRequest); err != nil {
		ctx.Log.Error(
			"Invalid request",
			map[string]interface{}{
				"err": err,
			},
		)
		w.WriteHeader(http.StatusBadRequest)
		return err
	}
	maxScore := &big.Rat{}
	for _, literalCase := range ephemeralRunRequest.Input.Cases {
		maxScore.Add(maxScore, literalCase.Weight)
	}
	inputFactory, err := common.NewLiteralInputFactory(
		ephemeralRunRequest.Input,
		ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		inputFactoryErr := err
		ctx.Log.Error(
			"Error creating input factory",
			map[string]interface{}{
				"err": inputFactoryErr,
			},
		)
		multipartWriter := multipart.NewWriter(w)
		defer multipartWriter.Close()

		w.Header().Set("Content-Type", multipartWriter.FormDataContentType())
		w.WriteHeader(http.StatusOK)
		resultWriter, err := multipartWriter.CreateFormFile("details.json", "details.json")
		if err != nil {
			ctx.Log.Error(
				"Error sending details.json",
				map[string]interface{}{
					"err": err,
				},
			)
			return inputFactoryErr
		}
		errorString := inputFactoryErr.Error()
		fakeResult := runner.NewRunResult("CE", maxScore)
		fakeResult.CompileError = &errorString
		if err = json.NewEncoder(resultWriter).Encode(fakeResult); err != nil {
			ctx.Log.Error(
				"Error sending json",
				map[string]interface{}{
					"err": err,
				},
			)
		}
		return inputFactoryErr
	}

	runInfo := grader.NewRunInfo()
	runInfo.Run.InputHash = inputFactory.Hash()
	runInfo.Run.Language = ephemeralRunRequest.Language
	runInfo.Run.MaxScore = maxScore
	runInfo.Run.Source = ephemeralRunRequest.Source
	runInfo.Priority = grader.QueuePriorityEphemeral
	ephemeralToken, err := h.ephemeralRunManager.SetEphemeral(runInfo)
	if err != nil {
		ctx.Log.Error(
			"Error making run ephemeral",
			map[string]interface{}{
				"err": err,
			},
		)
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	committed := false
	defer func(committed *bool) {
		if *committed {
			return
		}
		err = runInfo.Artifacts.Clean()
		if err != nil {
			ctx.Log.Error(
				"Error cleaning up after run",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	}(&committed)

	inputRef, err := ctx.InputManager.Add(inputFactory.Hash(), inputFactory)
	if err != nil {
		ctx.Log.Error(
			"Error adding input",
			map[string]interface{}{
				"err": err,
			},
		)
		w.WriteHeader(http.StatusBadRequest)
		return err
	}
	runWaitHandle, err := runs.AddWaitableRun(&ctx.Context, runInfo, inputRef)
	if err != nil {
		ctx.Log.Error(
			"Failed to add run context",
			map[string]interface{}{
				"err": err,
			},
		)
		w.WriteHeader(http.StatusServiceUnavailable)
		return err
	}

	multipartWriter := multipart.NewWriter(w)
	defer multipartWriter.Close()

	w.Header().Set("Content-Type", multipartWriter.FormDataContentType())
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-OmegaUp-EphemeralToken", ephemeralToken)
	w.WriteHeader(http.StatusOK)

	// Send a field so that the reader can be notified that the run
	// has been accepted and will be queued.
	if flusher, ok := w.(http.Flusher); ok {
		multipartWriter.WriteField("status", "waiting")
		flusher.Flush()
	}

	ctx.Log.Info(
		"enqueued run",
		map[string]interface{}{
			"run": runInfo.Run,
		},
	)

	// Send another field so that the reader can be notified that the run has
	// been accepted and will be queued.
	if flusher, ok := w.(http.Flusher); ok {
		multipartWriter.WriteField("status", "queueing")
		flusher.Flush()
	}

	// Wait until a runner has picked the run up, or the run has been finished.
	select {
	case <-runWaitHandle.Running():
		if flusher, ok := w.(http.Flusher); ok {
			multipartWriter.WriteField("status", "running")
			flusher.Flush()
		}
		break
	case <-runWaitHandle.Ready():
	}
	<-runWaitHandle.Ready()

	// Run was successful, send all the files as part of the payload.
	filenames := []string{"logs.txt.gz", "files.zip", "details.json"}
	for _, filename := range filenames {
		fd, err := runInfo.Artifacts.Get(&ctx.Context, filename)
		if err != nil {
			ctx.Log.Error(
				"Error opening file",
				map[string]interface{}{
					"filename": filename,
					"err":      err,
				},
			)
			continue
		}
		defer fd.Close()
		resultWriter, err := multipartWriter.CreateFormFile(filename, filename)
		if err != nil {
			ctx.Log.Error(
				"Error sending file",
				map[string]interface{}{
					"filename": filename,
					"err":      err,
				},
			)
			continue
		}
		if _, err = io.Copy(resultWriter, fd); err != nil {
			ctx.Log.Error(
				"Error sending file",
				map[string]interface{}{
					"filename": filename,
					"err":      err,
				},
			)
			continue
		}
	}

	// Finally commit the run to the manager.
	if err = saveEphemeralRunRequest(ctx, runInfo, ephemeralRunRequest); err != nil {
		return err
	}
	h.ephemeralRunManager.Commit(runInfo)
	committed = true
	ctx.Log.Info(
		"Finished running ephemeral run",
		map[string]interface{}{
			"token": ephemeralToken,
		},
	)

	return nil
}

func (h *ephemeralRunHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := h.ctx.Wrap(r.Context())
	ctx.Log.Info(
		"ephemeral run request",
		map[string]interface{}{
			"path": r.URL.Path,
		},
	)
	tokens := strings.Split(r.URL.Path, "/")

	if len(tokens) == 5 && tokens[3] == "new" && tokens[4] == "" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		runs, err := ctx.QueueManager.Get(grader.DefaultQueueName)
		if err != nil {
			ctx.Log.Error(
				"Failed to get default queue",
				map[string]interface{}{
					"err": err,
				},
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var ephemeralRunRequest grader.EphemeralRunRequest
		if err = json.NewDecoder(r.Body).Decode(&ephemeralRunRequest); err != nil {
			ctx.Log.Error(
				"Error decoding run request",
				map[string]interface{}{
					"err": err,
				},
			)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = h.addAndWaitForRun(ctx, w, &ephemeralRunRequest, runs)
		if err != nil {
			ctx.Log.Error(
				"Failed to perform ephemeral run",
				map[string]interface{}{
					"err": err,
				},
			)
		}
	} else if len(tokens) == 5 {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if _, ok := validEphemeralRunFilenames[tokens[4]]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runDirectory, ok := h.ephemeralRunManager.Get(tokens[3])
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		filename := path.Join(runDirectory, tokens[4])
		if _, err := os.Stat(filename + ".gz"); err == nil {
			if strings.HasSuffix(filename, ".txt") {
				w.Header().Add("Content-Type", "text/plain")
			} else if strings.HasSuffix(filename, ".json") {
				w.Header().Add("Content-Type", "application/json")
			}
			filename += ".gz"
			w.Header().Add("Content-Encoding", "gzip")
		}
		http.ServeFile(w, r, filename)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func registerEphemeralHandlers(
	ctx *grader.Context,
	mux *http.ServeMux,
	ephemeralRunManager *grader.EphemeralRunManager,
	tracing tracing.Provider,
) {
	ephemeralRunHandler := &ephemeralRunHandler{
		ephemeralRunManager: ephemeralRunManager,
		ctx:                 ctx,
	}
	mux.Handle(tracing.WrapHandle("/ephemeral/run/", ephemeralRunHandler))
}
