package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/omegaup/quark/broadcaster"
)

type trackingBody struct {
	io.Reader
	closed bool
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

type trackingTransport struct {
	statusCode int
	body       *trackingBody
}

func (t *trackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.statusCode,
		Status:     fmt.Sprintf("%d %s", t.statusCode, http.StatusText(t.statusCode)),
		Header:     make(http.Header),
		Body:       t.body,
		Request:    req,
	}, nil
}

func TestBroadcastClosesResponseBody(t *testing.T) {
	ctx := newGraderContext(t)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(path.Dir(ctx.Config.Grader.RuntimePath))
	}

	statusCodes := []int{
		http.StatusOK,
		http.StatusInternalServerError,
	}

	for _, statusCode := range statusCodes {
		body := &trackingBody{
			Reader: strings.NewReader(`{}`),
		}
		client := &http.Client{
			Transport: &trackingTransport{
				statusCode: statusCode,
				body:       body,
			},
		}

		message := &broadcaster.Message{
			Message: "test message",
		}

		_ = broadcast(ctx, client, message)

		if !body.closed {
			t.Errorf("broadcast() leaked response body for status code %d: resp.Body.Close() was not called", statusCode)
		}
	}
}
