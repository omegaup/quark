package common

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	base "github.com/omegaup/go-base/v2"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
)

func newTestInputManager(sizeLimit base.Byte) *InputManager {
	config := DefaultConfig()
	config.Logging.File = "stderr"
	config.Tracing.Enabled = false
	config.InputManager.CacheSize = sizeLimit

	ctx, err := NewContext(&config, "common")
	if err != nil {
		panic(err)
	}
	return NewInputManager(ctx)
}

type testInput struct {
	BaseInput
	valid bool
	size  int64
}

func (input *testInput) Verify() error {
	if !input.valid {
		return errors.New("Invalid input")
	}
	input.Commit(input.size)
	return nil
}

func (input *testInput) Delete() error {
	return nil
}

func (input *testInput) Release() {
	input.Delete()
}

func (input *testInput) Persist() error {
	return ErrUnimplemented
}

type testInputFactory struct {
	size int64
}

func (factory *testInputFactory) NewInput(hash string, mgr *InputManager) Input {
	return &testInput{
		BaseInput: *NewBaseInput(hash, mgr),
		size:      factory.size,
		valid:     true,
	}
}

func (factory *testInputFactory) Validate(hash string, mgr *InputManager) Input {
	return &testInput{
		BaseInput: *NewBaseInput(hash, mgr),
		size:      factory.size,
	}
}

type testCachedInputFactory struct {
	path string
}

func (factory *testCachedInputFactory) NewInput(
	hash string,
	mgr *InputManager,
) Input {
	contents, err := ioutil.ReadFile(path.Join(
		factory.path,
		fmt.Sprintf("%s/%s.prob.valid", hash[:2], hash[2:]),
	))
	if err != nil {
		panic(err)
	}
	return &testInput{
		BaseInput: *NewBaseInput(hash, mgr),
		valid:     string(contents) == "1",
	}
}

func (factory *testCachedInputFactory) GetInputHash(
	dirname string,
	info os.FileInfo,
) (string, bool) {
	return fmt.Sprintf(
		"%s%s",
		path.Base(dirname),
		strings.TrimSuffix(info.Name(), ".prob"),
	), strings.HasSuffix(info.Name(), ".prob")
}

func TestInputManagerSerializability(t *testing.T) {
	inputManager := newTestInputManager(base.Kibibyte)
	inputManager.MarshalJSON()
}

func TestInputManager(t *testing.T) {
	inputManager := newTestInputManager(base.Kibibyte)

	if inputManager.Size() != 0 {
		t.Errorf("InputManager.Size() == %d, want %d", inputManager.Size(), 0)
	}

	// Add an input (hash = 0)
	if inputRef, err := inputManager.Add("0", &CacheOnlyInputFactoryForTesting{}); err == nil {
		inputRef.Release()
		t.Errorf("InputManager.Add(\"0\", &CacheOnlyInputFactoryForTesting{}) == %q, want !nil", err)
	}
	inputRef, err := inputManager.Add("0", &testInputFactory{size: 1024})
	if err != nil {
		t.Errorf("InputManager.Add(\"0\") failed with %q", err)
	}
	if !inputRef.Input.Committed() {
		t.Errorf("Input.Committed() == %t, want %t", inputRef.Input.Committed(), true)
	}
	if inputManager.Size() != 1024 {
		t.Errorf("InputManager.Size() == %d, want %d", inputManager.Size(), 1024)
	}
	inputRef.Release()

	if inputManager.Size() != 1024 {
		t.Errorf("InputManager.Size() == %d, want %d", inputManager.Size(), 1024)
	}

	inputRef, err = inputManager.Add("0", &CacheOnlyInputFactoryForTesting{})
	if err != nil {
		t.Errorf("InputManager.Add(\"0\", &CacheOnlyInputFactoryForTesting{}) == %q, want nil", err)
	}
	inputRef.Release()

	// Add a new input (hash = 1)
	if inputRef, err := inputManager.Add("1", &CacheOnlyInputFactoryForTesting{}); err == nil {
		inputRef.Release()
		t.Errorf("InputManager.Add(\"1\", &CacheOnlyInputFactoryForTesting{}) == %q, want !nil", err)
	}
	inputRef, err = inputManager.Add("1", &testInputFactory{size: 1024})
	if err != nil {
		t.Errorf("InputManager.Add(\"1\") failed with %q", err)
	}
	inputRef.Release()

	// This should evict the old input and make it not accessible anymore.
	if inputManager.Size() != 1024 {
		t.Errorf("InputManager.Size() == %d, want %d", inputManager.Size(), 1024)
	}
	if inputRef, err := inputManager.Add("0", &CacheOnlyInputFactoryForTesting{}); err == nil {
		inputRef.Release()
		t.Errorf("InputManager.Add(\"0\", &CacheOnlyInputFactoryForTesting{}) == %q, want !nil", err)
	}
}

func TestPreloadInputs(t *testing.T) {
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		t.Fatalf("ioutil.TempDir failed with %q", err)
	}
	defer os.RemoveAll(dirname)

	// Create two Inputs.
	inputfiles := []struct {
		filename, contents string
	}{
		{"00/0.prob", ""},
		{"00/0.prob.valid", "0"},
		{"00/1.prob", ""},
		{"00/1.prob.valid", "1"},
	}
	for _, ift := range inputfiles {
		if err := os.MkdirAll(
			path.Join(dirname, path.Dir(ift.filename)),
			0755,
		); err != nil {
			t.Fatalf("Failed to create directory: %q", err)
		}
		if err := ioutil.WriteFile(
			path.Join(dirname, ift.filename),
			[]byte(ift.contents),
			0644,
		); err != nil {
			t.Fatalf("ioutil.WriteFile failed with %q", err)
		}
	}
	inputManager := newTestInputManager(base.Kibibyte)
	if err := inputManager.PreloadInputs(
		dirname,
		&testCachedInputFactory{path: dirname},
		&sync.Mutex{},
	); err != nil {
		t.Fatalf("InputManager.PreloadInputs failed with %q", err)
	}

	// 000 was invalid, so it must not exist.
	if inputRef, err := inputManager.Add("000", &CacheOnlyInputFactoryForTesting{}); err == nil {
		inputRef.Release()
		t.Errorf("InputManager.Add(\"000\") == %q, want !nil", err)
	}
	// 001 was valid, so it must exist.
	if _, err := inputManager.Add("001", &CacheOnlyInputFactoryForTesting{}); err != nil {
		t.Errorf("InputManager.Add(\"001\") == %q, want nil", err)
	}
}

func TestHashReader(t *testing.T) {
	hashtests := []struct {
		in, out string
	}{
		{"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{"hello, world!", "1f09d30c707d53f3d16c530dd73d70a6ce7596a9"},
	}

	for _, tt := range hashtests {
		inbuf := bytes.NewBufferString(tt.in)
		var outbuf bytes.Buffer
		hashreader := NewHashReader(inbuf, sha1.New())
		if _, err := io.Copy(&outbuf, hashreader); err != nil {
			t.Fatalf("io.Copy failed with %q", err)
		}
		if outbuf.String() != tt.in {
			t.Errorf("HashReader read %q, want %q", outbuf.String(), tt.in)
		}
		readhash := fmt.Sprintf("%02x", hashreader.Sum(nil))
		if readhash != tt.out {
			t.Errorf("HashReader hashed %q, want %q", readhash, tt.out)
		}
	}
}

func TestSha1Sum(t *testing.T) {
	hashtests := []struct {
		in, out string
	}{
		{"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{"hello, world!", "1f09d30c707d53f3d16c530dd73d70a6ce7596a9"},
	}

	for _, tt := range hashtests {
		f, err := ioutil.TempFile("/tmp", "testsha1sum")
		if err != nil {
			t.Fatalf("ioutil.TempFile failed with %q", err)
		}
		defer os.Remove(f.Name())
		if _, err := f.WriteString(tt.in); err != nil {
			t.Fatalf("io.Copy failed with %q", err)
		}
		f.Close()
		hash, err := Sha1sum(f.Name())
		if err != nil {
			t.Fatalf("Sha1sum failed with %q", err)
		}
		readhash := fmt.Sprintf("%02x", hash)
		if readhash != tt.out {
			t.Errorf("Sha1sum hashed %q, want %q", readhash, tt.out)
		}
	}
}
