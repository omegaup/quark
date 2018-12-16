package common

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestRotatingFile(t *testing.T) {
	dirname, err := ioutil.TempDir("/tmp", "testsha1sum")
	if err != nil {
		t.Fatalf("ioutil.TempDir failed with %v", err)
	}
	defer os.RemoveAll(dirname)

	logFilename := path.Join(dirname, "log")
	logFile, err := NewRotatingFile(logFilename, 0644, NopOpenedCallback)
	if err != nil {
		t.Fatalf("NewRotatingFile failed with %v", err)
	}
	defer logFile.Close()
	logFile.WriteString("hello, ")
	logFile.Rotate()
	logFile.WriteString("world!")

	oldLogFilename := logFilename + ".old"
	if err := os.Rename(logFilename, oldLogFilename); err != nil {
		t.Fatalf("Rename failed with %v", err)
	}
	logFile.WriteString("\n")
	logFile.Rotate()

	logFile.WriteString("hi!\n")
	logFile.Close()

	for filename, expectedContents := range map[string]string{
		oldLogFilename: "hello, world!\n",
		logFilename:    "hi!\n",
	} {
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatalf("ReadFile(%s) failed with %v", filename, err)
		}
		if string(contents) != expectedContents {
			t.Errorf("Contents of %s were %q, expected %q", filename, string(contents), expectedContents)
		}
	}
}
