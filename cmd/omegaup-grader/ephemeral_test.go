package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"
)

func newGraderContext(t *testing.T) *grader.Context {
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %s", err)
	}
	ctx, err := grader.NewContext(bytes.NewBufferString(
		fmt.Sprintf(
			"{"+
				"\"Logging\": {\"File\": \"stderr\"}, "+
				"\"Tracing\": {\"Enabled\": false}, "+
				"\"InputManager\": {\"CacheSize\": 1024}, "+
				"\"Grader\": {"+
				"  \"RuntimePath\": %q, "+
				"  \"Ephemeral\": {\"EphemeralSizeLimit\": 1024}"+
				"}, "+
				"\"Runner\": {"+
				"  \"RuntimePath\": %q"+
				"}"+
				"}",
			path.Join(dirname, "grader"),
			path.Join(dirname, "runner"),
		),
	))
	if err != nil {
		t.Fatalf("Failed to create context: %s", err)
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

	return ctx
}

func TestEphemeralGrader(t *testing.T) {
	ctx := newGraderContext(t)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(path.Dir(ctx.Config.Grader.RuntimePath))
	}
	mux := http.NewServeMux()
	registerEphemeralHandlers(ctx, mux)
	registerRunnerHandlers(ctx, mux, nil, true)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	go func() {
		requestURL, err := url.Parse(ts.URL + "/run/request/")
		if err != nil {
			panic(err)
		}
		resp, err := ts.Client().Get(requestURL.String())
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		decoder := json.NewDecoder(resp.Body)
		var run common.Run
		if err := decoder.Decode(&run); err != nil {
			panic(err)
		}
		uploadURL, err := url.Parse(fmt.Sprintf("%s/run/%d/results/", ts.URL, run.AttemptID))
		if err != nil {
			panic(err)
		}

		var buf bytes.Buffer
		var contentType string
		{
			multipartWriter := multipart.NewWriter(&buf)
			contentType = multipartWriter.FormDataContentType()
			filesWriter, err := multipartWriter.CreateFormFile("file", "files.zip")
			if err != nil {
				panic(err)
			}

			inputManager := common.NewInputManager(&ctx.Context)
			baseURL, err := url.Parse(ts.URL)
			if err != nil {
				panic(err)
			}
			input, err := inputManager.Add(
				run.InputHash,
				runner.NewInputFactory(ts.Client(), &ctx.Config, baseURL),
			)
			if err != nil {
				panic(err)
			}
			defer input.Release(input)

			result, err := runner.Grade(&ctx.Context, filesWriter, &run, input, &runner.NoopSandbox{})
			if err != nil {
				panic(err)
			}

			runner.NoopSandboxFixupResult(result)

			resultWriter, err := multipartWriter.CreateFormFile("file", "details.json")
			if err != nil {
				panic(err)
			}
			encoder := json.NewEncoder(resultWriter)
			if err := encoder.Encode(result); err != nil {
				panic(err)
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
			panic(err)
		}
		response.Body.Close()
	}()

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
