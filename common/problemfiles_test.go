package common

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"
)

func testProblemFiles(t *testing.T, f ProblemFiles) {
	t.Helper()

	contentsWant := []byte("1 2\n")
	contents, err := f.GetContents("cases/0.in")
	if err != nil {
		t.Errorf("Failed to get contents of \"cases/0.in\": %v", err)
	} else if !bytes.Equal(contentsWant, contents) {
		t.Errorf("Failed to get contents of \"cases/0.in\": want %q; got %q", contentsWant, contents)
	}

	stringContentsWant := "3\n"
	stringContents, err := f.GetStringContents("cases/0.out")
	if err != nil {
		t.Errorf("Failed to get contents of \"cases/0.out\": %v", err)
	} else if stringContentsWant != stringContents {
		t.Errorf("Failed to get contents of \"cases/0.out\": want %q; got %q", stringContentsWant, stringContents)
	}

	var buf bytes.Buffer
	fh, err := f.Open("cases/0.in")
	if err != nil {
		t.Errorf("Failed to get contents of \"cases/0.in\": %v", err)
		return
	}
	defer fh.Close()

	if _, err := io.Copy(&buf, fh); err != nil {
		t.Errorf("Failed to get contents of \"cases/0.in\": %v", err)
	} else if !bytes.Equal(contentsWant, buf.Bytes()) {
		t.Errorf("Failed to get contents of \"cases/0.in\": want %q; got %q", contentsWant, buf.Bytes())
	}

	_, err = f.GetContents("this file does not exist")
	if !os.IsNotExist(err) {
		t.Errorf("File unexpectedly found: want os.ErrNotExist; got %q", err)
	}

	expectedFiles := []string{"cases/0.in", "cases/0.out", "settings.json"}
	if !reflect.DeepEqual(expectedFiles, f.Files()) {
		t.Errorf("Failed to get file list: want %v; got %v", expectedFiles, f.Files())
	}
}

func TestProblemFilesFromFilesystem(t *testing.T) {
	f, err := NewProblemFilesFromFilesystem("testdata/problemfiles/filesystem")
	if err != nil {
		t.Fatalf("Failed to open filesystem problem: %v", err)
	}
	defer f.Close()

	testProblemFiles(t, f)
}

func TestProblemFilesFromGit(t *testing.T) {
	f, err := NewProblemFilesFromGit(
		"testdata/problemfiles/git",
		"57b650b824bc99f187dcfccdb408b9c320fdb834",
	)
	if err != nil {
		t.Fatalf("Failed to open git repository: %v", err)
	}
	defer f.Close()

	testProblemFiles(t, f)
}

func TestProblemFilesFromMap(t *testing.T) {
	f := NewProblemFilesFromMap(
		map[string]string{
			"settings.json": "{}",
			"cases/0.in":    "1 2\n",
			"cases/0.out":   "3\n",
		},
		":memory:",
	)
	defer f.Close()

	testProblemFiles(t, f)
}

func TestProblemFilesFromZip(t *testing.T) {
	zip, err := zip.OpenReader("testdata/problemfiles/zipFile.zip")
	if err != nil {
		t.Fatalf("Failed to open zip file: %v", err)
	}
	defer zip.Close()

	f := NewProblemFilesFromZip(
		&zip.Reader,
		"testdata/problemfiles/zipFile.zip",
	)
	defer f.Close()

	testProblemFiles(t, f)
}

func TestProblemFilesFromChain(t *testing.T) {
	f := NewProblemFilesFromChain(
		NewProblemFilesFromMap(
			map[string]string{
				"cases/0.in": "1 2\n",
			},
			":memory:",
		),
		NewProblemFilesFromMap(
			map[string]string{
				"settings.json": "{}",
				"cases/0.out":   "3\n",
			},
			":memory:",
		),
	)
	defer f.Close()

	testProblemFiles(t, f)
}
