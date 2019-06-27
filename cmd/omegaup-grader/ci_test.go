package main

import (
	"context"
	"encoding/json"
	"github.com/omegaup/quark/grader"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

func readReport(
	t *testing.T,
	ctx *grader.Context,
	client *http.Client,
	url string,
	report *CIReport,
	excludedStates []CIState,
) {
	// There is no synchronization between when the ephemeral grader finishes
	// running and the CI commits the updated results to disk, so we need to poll
	// for a bit.
	for i := 0; i < 10; i++ {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to create request: %s", err)
		}
		if err := json.NewDecoder(resp.Body).Decode(report); err != nil {
			t.Fatalf("Failed to deserialize report: %s", err)
		}
		resp.Body.Close()

		ctx.Log.Info("Getting results...", "report", report, "round", i+1)

		if len(report.Tests) > 0 {
			found := false
			for _, excludedState := range excludedStates {
				if report.State == excludedState {
					found = true
					break
				}
			}
			if !found {
				return
			}
		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
	t.Fatalf("Run did not finish within 5 seconds")
}

func TestCI(t *testing.T) {
	ctx := newGraderContext(t)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(path.Dir(ctx.Config.Grader.RuntimePath))
	}
	if problemsGitPath, err := filepath.Abs("testdata/problems.git"); err != nil {
		t.Fatalf("Failed to get path of problems.git: %s", err)
	} else {
		if err := os.Symlink(
			problemsGitPath,
			path.Join(ctx.Config.Grader.RuntimePath, "problems.git"),
		); err != nil {
			t.Fatalf("Failed to setup problems.git: %s", err)
		}
	}
	ephemeralRunManager := grader.NewEphemeralRunManager(ctx)
	if err := ephemeralRunManager.Initialize(); err != nil {
		t.Fatalf("Failed to fully initalize the ephemeral run manager: %s", err)
	}
	mux := http.NewServeMux()
	shutdowner := registerCIHandlers(ctx, mux, ephemeralRunManager)
	defer shutdowner.Shutdown(context.Background())
	registerRunnerHandlers(ctx, mux, nil, true)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	requestURL, err := url.Parse(ts.URL + "/ci/problem/sumas/1e3dfc5facb54337315febc6b965cb89bba79d9c/")
	if err != nil {
		t.Fatalf("Failed to parse URL: %s", err)
	}

	var report CIReport

	readReport(t, ctx, ts.Client(), requestURL.String(), &report, []CIState{})

	for range report.Tests {
		ctx.Log.Info("Gonna request a run")
		RunnerRequestRun(t, ctx, ts)
	}

	readReport(t, ctx, ts.Client(), requestURL.String(), &report, []CIState{CIStateWaiting, CIStateRunning})

	// Since the no-op runner always returns AC, it fails the PA test.
	if report.State != CIStateFailed {
		t.Errorf("report.State == %q, want %q", report.State, CIStateFailed)
	}
	for _, test := range report.Tests {
		if test.Filename == "solutions/PA.cpp" {
			if test.State != CIStateFailed {
				t.Errorf("%s: test.State == %q, want %q", test.Filename, test.State, CIStateFailed)
			}
		} else {
			if test.State != CIStatePassed {
				t.Errorf("%s: test.State == %q, want %q", test.Filename, test.State, CIStatePassed)
			}
		}
	}
}
