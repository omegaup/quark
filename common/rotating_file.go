package common

import (
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// OpenedCallback allows the caller to specify an action to be performed when
// the file is opened and before it is available for writing.
type OpenedCallback func(f *os.File, isEmpty bool) error

// NopOpenedCallback is an OpenedCallback that does nothing.
func NopOpenedCallback(*os.File, bool) error { return nil }

// A RotatingFile is an io.WriteCloser that supports reopening through SIGHUP.
// It opens the underlying file in append-only mode. All
// operations are thread-safe.
type RotatingFile struct {
	file          *os.File
	signalChannel chan<- os.Signal
	reopen        func() (*os.File, error)
	lock          sync.Mutex
}

var _ io.WriteCloser = &RotatingFile{}

func openFile(path string, mode os.FileMode, callback OpenedCallback) (*os.File, error) {
	file, err := os.OpenFile(
		path,
		os.O_WRONLY|os.O_APPEND|os.O_CREATE,
		mode,
	)
	if err != nil {
		return nil, err
	}
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	if err := callback(file, pos == 0); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

// NewRotatingFile opens path for writing in append-only mode and listens for
// SIGHUP so that it can reopen the file automatically.
func NewRotatingFile(path string, mode os.FileMode, callback OpenedCallback) (*RotatingFile, error) {
	file, err := openFile(path, mode, callback)
	if err != nil {
		return nil, err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	r := &RotatingFile{
		file:          file,
		signalChannel: c,
		reopen:        func() (*os.File, error) { return openFile(path, mode, callback) },
	}

	go func() {
		for range c {
			r.Rotate()
		}
	}()

	return r, nil
}

// Write writes the bytes into the underlying file.
func (r *RotatingFile) Write(b []byte) (int, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	return r.file.Write(b)
}

// WriteString is like Write, but writes the contents of string s rather than a
// slice of bytes.
func (r *RotatingFile) WriteString(s string) (int, error) {
	defer r.lock.Unlock()
	r.lock.Lock()
	return r.file.WriteString(s)
}

// Close closes the underlying file and stops listening for SIGHUP.
func (r *RotatingFile) Close() error {
	defer r.lock.Unlock()
	r.lock.Lock()
	signal.Stop(r.signalChannel)
	return r.file.Close()
}

// Rotate reopens the file and closes the previous one.
func (r *RotatingFile) Rotate() error {
	newFile, err := r.reopen()
	if err != nil {
		return err
	}

	defer r.lock.Unlock()
	r.lock.Lock()
	oldFile := r.file
	r.file = newFile
	oldFile.Close()
	return nil
}
