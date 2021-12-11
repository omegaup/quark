package common

import (
	"bytes"
	"strings"
	"testing"
)

func newTestingContext() *Context {
	ctx, err := NewContextFromReader(bytes.NewBufferString(
		"{\"Logging\": {\"File\": \"stderr\"}, \"Tracing\": {\"Enabled\": false}}",
	), "common")
	if err != nil {
		panic(err)
	}
	return ctx
}

func TestDebugContext(t *testing.T) {
	ctx := newTestingContext()
	defer ctx.Close()
	dbg := ctx.DebugContext(nil)
	// This should not be added to the Buffer.
	ctx.Log.Error("Critical error", nil)
	// This should be.
	dbg.Log.Debug("Debug statement", nil)

	logStr := string(dbg.LogBuffer())
	if strings.Index(logStr, "Critical error") != -1 {
		t.Errorf("\"Critical error\" present in LogBuffer: %q", logStr)
	}
	if strings.Index(logStr, "Debug statement") == -1 {
		t.Errorf("\"Debug statement\" not present in LogBuffer: %q", logStr)
	}

	traceStr := string(dbg.TraceBuffer())
	if len(traceStr) == 0 {
		t.Errorf("Tracing string empty")
	}
}

func TestConfigSerializability(t *testing.T) {
	ctx := newTestingContext()
	defer ctx.Close()
	serializedConfig := ctx.Config.String()
	if len(serializedConfig) == 0 {
		t.Errorf("Serialized config empty")
	}
}
