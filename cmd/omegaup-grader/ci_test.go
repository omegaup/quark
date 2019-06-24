package main

import (
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
	registerCIHandlers(ctx, mux, ephemeralRunManager)
	registerRunnerHandlers(ctx, mux, nil, true)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	requestURL, err := url.Parse(ts.URL + "/ci/problem/sumas/1e3dfc5facb54337315febc6b965cb89bba79d9c/")
	if err != nil {
		t.Fatalf("Failed to parse URL: %s", err)
	}

	var report CIReport
	{
		resp, err := ts.Client().Get(requestURL.String())
		if err != nil {
			t.Fatalf("Failed to create request: %s", err)
		}
		if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
			t.Fatalf("Failed to deserialize report: %s", err)
		}
		resp.Body.Close()

		ctx.Log.Info("Initial results", "report", report)
	}

	for range report.Tests {
		RunnerRequestRun(t, ctx, ts)
	}

	// There is no synchronization between when the ephemeral grader finishes
	// running and the CI commits the updated results to disk, so we need to poll
	// for a bit.
	finished := false
	for i := 0; i < 10; i++ {
		resp, err := ts.Client().Get(requestURL.String())
		if err != nil {
			t.Fatalf("Failed to create request: %s", err)
		}
		if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
			t.Fatalf("Failed to deserialize report: %s", err)
		}
		resp.Body.Close()

		ctx.Log.Info("Getting results...", "report", report, "round", i+1)

		if report.State != CIStateWaiting && report.State != CIStateRunning {
			finished = true
			break
		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
	if !finished {
		t.Fatalf("Run did not finish")
	}

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
