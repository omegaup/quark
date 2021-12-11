package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/omegaup/go-base/v3/tracing"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
)

func newGraderContext(t *testing.T) *grader.Context {
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %s", err)
	}
	ctx, err := grader.NewContext(bytes.NewBufferString(
		fmt.Sprintf(
			`{
				"DB": {
					"Driver": "sqlite3",
					"DataSourceName": ":memory:"
				},
				"Logging": {"File": "stderr"},
				"Tracing": {"Enabled": false},
				"InputManager": {"CacheSize": 1048576},
				"Grader": {
				  "RuntimePath": %q,
				  "Ephemeral": {"EphemeralSizeLimit": 1048576}
				},
				"Runner": {
				  "RuntimePath": %q
				}
			}`,
			path.Join(dirname, "grader"),
			path.Join(dirname, "runner"),
		),
	))
	if err != nil {
		t.Fatalf("Failed to create context: %s", err)
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

	if err := os.MkdirAll(ctx.Config.Grader.RuntimePath, 0755); err != nil {
		t.Fatalf("Failed to create the grader runtime directory: %s", err)
	}
	if err := os.MkdirAll(ctx.Config.Runner.RuntimePath, 0755); err != nil {
		t.Fatalf("Failed to create the runner runtime directory: %s", err)
	}

	return ctx
}

func RunnerRequestRun(t *testing.T, ctx *grader.Context, ts *httptest.Server) {
	baseURL, err := url.Parse(ts.URL)
	if err != nil {
		panic(err)
	}
	requestURL, err := url.Parse(ts.URL + "/run/request/")
	if err != nil {
		panic(err)
	}
	resp, err := ts.Client().Get(requestURL.String())
	if err != nil {
		t.Fatalf("Failed to request a run: %s", err)
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var run common.Run
	if err := decoder.Decode(&run); err != nil {
		t.Fatalf("Failed to decode run request: %s", err)
	}
	uploadURL, err := url.Parse(fmt.Sprintf("%s/run/%d/results/", ts.URL, run.AttemptID))
	if err != nil {
		t.Fatalf("Failed to parse result URL: %s", err)
	}

	var buf bytes.Buffer
	var contentType string
	{
		multipartWriter := multipart.NewWriter(&buf)
		contentType = multipartWriter.FormDataContentType()
		filesWriter, err := multipartWriter.CreateFormFile("file", "files.zip")
		if err != nil {
			t.Fatalf("Failed to create files.zip multipart writer: %s", err)
		}

		inputManager := common.NewInputManager(&ctx.Context)
		inputRef, err := inputManager.Add(
			run.InputHash,
			runner.NewInputFactory(ts.Client(), &ctx.Config, baseURL, ""),
		)
		if err != nil {
			t.Fatalf("Failed to add input to input manager: %s", err)
		}
		defer inputRef.Release()

		result, err := runner.Grade(&ctx.Context, filesWriter, &run, inputRef.Input, &runner.NoopSandbox{})
		if err != nil {
			t.Fatalf("Failed to grade run: %s", err)
		}

		runner.NoopSandboxFixupResult(result)

		resultWriter, err := multipartWriter.CreateFormFile("file", "details.json")
		if err != nil {
			t.Fatalf("Failed to create details.json multipart writer: %s", err)
		}
		if err := json.NewEncoder(resultWriter).Encode(result); err != nil {
			t.Fatalf("Failed to write run results: %s", err)
		}
		multipartWriter.Close()
	}

	req := &http.Request{
		Method: "POST",
		URL:    uploadURL,
		Header: map[string][]string{
			"Content-Type": {contentType},
		},
		Body: ioutil.NopCloser(&buf),
	}
	response, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Failed to upload final results: %s", err)
	}
	response.Body.Close()
}

func TestEphemeralGrader(t *testing.T) {
	ctx := newGraderContext(t)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(path.Dir(ctx.Config.Grader.RuntimePath))
	}
	ephemeralRunManager := grader.NewEphemeralRunManager(ctx)
	if err := ephemeralRunManager.Initialize(); err != nil {
		t.Fatalf("Failed to fully initalize the ephemeral run manager: %s", err)
	}
	mux := http.NewServeMux()
	tracing := tracing.NewNoOpProvider()
	registerEphemeralHandlers(ctx, mux, ephemeralRunManager, tracing)
	registerRunnerHandlers(ctx, mux, nil, true, tracing)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	go RunnerRequestRun(t, ctx, ts)

	prePushURL, err := url.Parse(ts.URL + "/ephemeral/run/new/")
	if err != nil {
		panic(err)
	}
	req := &http.Request{
		Method: "POST",
		URL:    prePushURL,
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: ioutil.NopCloser(bytes.NewBufferString(`
			{
				"source": "print 3",
				"language": "py",
				"input": {
					"cases": {
						"0": {
							"in": "1 2",
							"out": "3",
							"weight": 1
						},
						"1": {
							"in": "2 3",
							"out": "5",
							"weight": 1
						}
					}
				}
			}
		`)),
	}
	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Failed to create ephemeral run request: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Failed to request ephemeral run: Status %v, headers: %v", res.StatusCode, res.Header)
	}

	if _, err := ioutil.ReadAll(res.Body); err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}
}
