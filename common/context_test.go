package common

import (
	"bytes"
	"strings"
	"testing"
	"time"
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
	dbg := ctx.DebugContext()
	// This should not be added to the Buffer.
	ctx.Log.Error("Critical error")
	// This should be.
	dbg.Log.Debug("Debug statement")

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
	ctx.Config.String()
}

func TestDuration(t *testing.T) {
	d1 := Duration(time.Duration(30) * time.Second)
	serialized, err := d1.MarshalJSON()
	if err != nil {
		t.Fatalf(err.Error())
	}
	var d2 Duration
	if err = d2.UnmarshalJSON(serialized); err != nil {
		t.Fatalf(err.Error())
	}
	if d1 != d2 {
		t.Errorf("expected %v got %v", d1.String(), d2.String())
	}
}
