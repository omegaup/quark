package common

import (
	"bytes"
	"strings"
	"testing"
)

func TestDebugContext(t *testing.T) {
	ctx, err := NewContext(bytes.NewBufferString(
		"{\"Logging\": {\"File\": \"stderr\"}}",
	))
	if err != nil {
		panic(err)
	}
	dbg := ctx.DebugContext()
	// This should not be added to the Buffer.
	ctx.Log.Error("Critical error")
	// This should be.
	dbg.Log.Debug("Debug statement")

	str := dbg.Buffer.String()
	if strings.Index(str, "Critical error") != -1 {
		t.Errorf("\"Critical error\" present in Buffer: %q", str)
	}
	if strings.Index(str, "Debug statement") == -1 {
		t.Errorf("\"Debug statement\" not present in Buffer: %q", str)
	}
}
