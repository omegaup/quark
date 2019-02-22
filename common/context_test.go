package common

import (
	"bytes"
	"math/big"
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
	serializedConfig := ctx.Config.String()
	if len(serializedConfig) == 0 {
		t.Errorf("Serialized config empty")
	}
}

func TestParseRational(t *testing.T) {
	testTable := []struct {
		str      string
		expected *big.Rat
	}{
		{"1", big.NewRat(1, 1)},
		{"0.5", big.NewRat(1, 2)},
		{"0.333333333", big.NewRat(1, 3)},
		{"0.23", big.NewRat(23, 100)},
		{"0.023", big.NewRat(23, 1000)},
		{"0.208333333", big.NewRat(5, 24)},
		{"0.123456789", big.NewRat(63, 512)},
	}
	for _, entry := range testTable {
		var val *big.Rat
		var err error
		if val, err = ParseRational(entry.str); err != nil {
			t.Fatalf(err.Error())
		}
		if entry.expected.Cmp(val) != 0 {
			t.Errorf("expected %v got %v", entry.expected, val)
		}
	}
}
