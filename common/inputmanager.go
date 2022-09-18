package common

import (
	"container/list"
	"crypto/sha1"
	"encoding/json"
	stderrors "errors"
	"fmt"
	base "github.com/omegaup/go-base/v3"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sync"
)

var (
	// ErrUnimplemented is an error that is returned when a method is not implemented.
	ErrUnimplemented = stderrors.New("unimplemented")

	// ErrCacheOnlyInput is an error that is returned when a mutating method is
	// attempted on a CacheOnlyInput.
	ErrCacheOnlyInput = stderrors.New("cache-only input")
)

// Input represents a problem's input set.
//
// Input is reference-counted, so it will keep the input in memory (and disk)
// while there is at least one reference to it. Once the last reference is
// released, it will be inserted into its associated InputManager.
type Input interface {
	base.SizedEntry

	// Path returns the path to the uncompressed representation of the Input
	// on-disk.
	Path() string

	// Hash is the identifier of the Input. It is typically the Git hash of the
	// tree that it represents.
	Hash() string

	// Commited returns true if the input has been verified and is committed into
	// its InputManager.
	Committed() bool

	// Verify ensures that the version of the Input stored in the filesystem
	// exists and is consistent, and commits it so that it can be added to the
	// InputManager. It is expected that Size() returns a valid size after
	// Verify() returns successfully.
	Verify() error

	// Persist stores the Input into the filesystem in a form that is
	// easily consumed and can be verified for consistency after restarting.
	// The files can be garbage collected later if there are no outstanding
	// references to the input and there is disk space pressure.
	Persist() error

	// Delete removes the filesystem version of the Input to free space.
	Delete() error

	// Settings returns the problem's settings for the Input. Problems can have
	// different ProblemSettings on different Inputs.
	Settings() *ProblemSettings
}

// TransmittableInput is an input that can be transmitted over HTTP.
type TransmittableInput interface {
	Input

	// Transmit sends a serialized version of the Input over HTTP. It should be a
	// .tar.gz file with the Content-SHA1 header set to the hexadecimal
	// representation of the SHA-1 hash of the file.
	Transmit(http.ResponseWriter) error
}

// InputRef represents a reference to an Input
type InputRef struct {
	Input Input
	ref   *base.SizedEntryRef[Input]
	mgr   *InputManager
}

// Release marks the Input as not being used anymore, so it becomes eligible
// for eviction.
func (i *InputRef) Release() {
	i.mgr.lruCache.Put(i.ref)

	// Prevent double-release.
	i.Input = nil
	i.ref = nil
	i.mgr = nil
}

// BaseInput is a struct that provides most of the functions required to
// implement Input.
type BaseInput struct {
	sync.Mutex
	committed bool
	refcount  int
	size      base.Byte
	hash      string
	mgr       *InputManager
	settings  ProblemSettings
}

// NewBaseInput returns a new reference to a BaseInput.
func NewBaseInput(hash string, mgr *InputManager) *BaseInput {
	return &BaseInput{
		hash: hash,
		mgr:  mgr,
	}
}

// Path returns the filesystem path to the Input.
func (input *BaseInput) Path() string {
	return "/dev/null"
}

// Committed returns whether the Input has been committed to disk.
func (input *BaseInput) Committed() bool {
	return input.committed
}

// Size returns the total disk size used by the Input.
func (input *BaseInput) Size() base.Byte {
	return input.size
}

// Hash returns the hash of the Input.
func (input *BaseInput) Hash() string {
	return input.hash
}

// Commit adds the Input to the set of cached inputs, and also adds its size to
// the total committed size.
func (input *BaseInput) Commit(size int64) {
	input.size = base.Byte(size)
	input.committed = true
}

// Settings returns the ProblemSettings associated with the current Input.
func (input *BaseInput) Settings() *ProblemSettings {
	return &input.settings
}

// InputManager handles a pool of recently-used input sets. The pool has a
// fixed maximum size with a least-recently used eviction policy.
type InputManager struct {
	ctx      *Context
	lruCache *base.LRUCache[Input]
}

// inputEntry represents an entry in the InputManager.
type inputEntry struct {
	input       Input
	listElement *list.Element
}

// InputFactory creates Input objects for an InputManager based on a hash. The
// hash is just an opaque identifier that just so happens to be the SHA1 hash
// of the git tree representation of the Input.
//
// InputFactory is provided so that the Grader and the Runner can have
// different implementations of how to create, read, and write an Input in
// disk.
type InputFactory interface {
	NewInput(hash string, mgr *InputManager) Input
}

// CachedInputFactory is an InputFactory that also can validate Inputs that are
// already in the filesystem.
type CachedInputFactory interface {
	InputFactory

	// GetInputHash returns both the hash of the input for the specified
	// os.FileInfo. It returns ok = false in case the os.FileInfo does not refer
	// to an Input.
	GetInputHash(dirname string, info os.FileInfo) (hash string, ok bool)
}

// IdentityInputFactory is an InputFactory that only constructs one Input.
type IdentityInputFactory struct {
	input Input
}

// NewIdentityInputFactory returns an IdentityInputFactory associated with the
// provided Input.
func NewIdentityInputFactory(input Input) *IdentityInputFactory {
	return &IdentityInputFactory{
		input: input,
	}
}

// NewInput returns the Input provided in the constructor. Panics if the
// requested hash does not match the one of the Input.
func (factory *IdentityInputFactory) NewInput(hash string, mgr *InputManager) Input {
	if factory.input.Hash() != hash {
		panic(fmt.Errorf(
			"Invalid hash for IdentityInputFactory. Expected %s got %s",
			factory.input.Hash(),
			hash,
		))
	}
	return factory.input
}

// CacheOnlyInputFactoryForTesting is an InputFactory that does not know how to
// create an input. This means that if it's not found already on the cache, the
// operation will fail.
type CacheOnlyInputFactoryForTesting struct {
}

// NewInput returns the cacheOnlyInput.
func (factory *CacheOnlyInputFactoryForTesting) NewInput(hash string, mgr *InputManager) Input {
	return &cacheOnlyInput{}
}

// cacheOnlyInput is an Input that cannot be created, only obtained from the cache.
type cacheOnlyInput struct {
}

func (input *cacheOnlyInput) Size() base.Byte {
	return 0
}

func (input *cacheOnlyInput) Release() {
}

func (input *cacheOnlyInput) Path() string {
	return "/dev/null"
}

func (input *cacheOnlyInput) Hash() string {
	return "0000000000000000000000000000000000000000"
}

func (input *cacheOnlyInput) Committed() bool {
	return false
}

func (input *cacheOnlyInput) Verify() error {
	return ErrCacheOnlyInput
}

func (input *cacheOnlyInput) Persist() error {
	return ErrCacheOnlyInput
}

func (input *cacheOnlyInput) Delete() error {
	return ErrCacheOnlyInput
}

func (input *cacheOnlyInput) Settings() *ProblemSettings {
	return &ProblemSettings{}
}

// NewInputManager creates a new InputManager with the provided Context.
func NewInputManager(ctx *Context) *InputManager {
	return &InputManager{
		ctx:      ctx,
		lruCache: base.NewLRUCache[Input](base.Byte(ctx.Config.InputManager.CacheSize)),
	}
}

// Add associates an opaque identifier (the hash) with an Input in the
// InputManager.
//
// The InputFactory is responsible to create the Input if it has not yet been
// created. The Input will be validated if it has not been committed to the
// InputManager, but will still not be accounted into the size limit since
// there is at least one live reference to it.
//
// InputRef.Release() must be called once the input is no longer needed so that
// it can be garbage-collected.
func (mgr *InputManager) Add(hash string, factory InputFactory) (*InputRef, error) {
	entryRef, err := mgr.lruCache.Get(
		hash,
		func(hash string) (Input, error) {
			input := factory.NewInput(hash, mgr)
			if input.Committed() {
				// No further processing necessary.
				return input, nil
			}
			// This operation can take a while.
			if err := input.Verify(); err != nil {
				mgr.ctx.Log.Warn(
					"Hash verification failed. Regenerating",
					map[string]any{
						"path": input.Hash(),
						"err":  err,
					},
				)
				input.Delete()

				if err := input.Persist(); err != nil {
					mgr.ctx.Log.Error(
						"Error creating archive",
						map[string]any{
							"hash": input.Hash(),
							"err":  err,
						},
					)
					return nil, err
				}
				mgr.ctx.Log.Info(
					"Generated input",
					map[string]any{
						"hash": input.Hash(),
						"size": input.Size(),
					},
				)
			} else {
				mgr.ctx.Log.Debug(
					"Reusing input",
					map[string]any{
						"hash": input.Hash(),
						"size": input.Size(),
					},
				)
			}
			return input, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return &InputRef{
		Input: entryRef.Value,
		mgr:   mgr,
		ref:   entryRef,
	}, nil
}

// Size returns the total size of Inputs in the InputManager.
func (mgr *InputManager) Size() base.Byte {
	return mgr.lruCache.Size()
}

// PreloadInputs reads all files in path, runs them through the specified
// filter, and tries to add them into the InputManager. PreloadInputs acquires
// the ioLock just before doing I/O in order to guarantee that the system will
// not be doing expensive I/O operations in the middle of a
// performance-sensitive operation (like running contestants' code).
func (mgr *InputManager) PreloadInputs(
	rootdir string,
	factory CachedInputFactory,
	ioLock sync.Locker,
) error {
	// Since all the filenames in the cache directory are (or contain) the hash,
	// it is useful to introduce 256 intermediate directories with the first two
	// nibbles of the hash to avoid the cache directory entry to grow too large
	// and become inefficient.
	for i := 0; i < 256; i++ {
		dirname := path.Join(rootdir, fmt.Sprintf("%02x", i))
		contents, err := ioutil.ReadDir(dirname)
		if err != nil {
			continue
		}
		for _, info := range contents {
			hash, ok := factory.GetInputHash(dirname, info)
			if !ok {
				continue
			}

			// Make sure no other I/O is being made while we pre-fetch this input.
			ioLock.Lock()
			inputRef, err := mgr.Add(hash, factory)
			if err != nil {
				os.RemoveAll(path.Join(dirname, info.Name()))
				mgr.ctx.Log.Error(
					"Cached input corrupted",
					map[string]any{
						"hash": hash,
					},
				)
			} else {
				inputRef.Release()
			}
			ioLock.Unlock()
		}
	}
	mgr.ctx.Log.Info(
		"Finished preloading cached inputs",
		map[string]any{
			"cache_size": mgr.Size(),
		},
	)
	return nil
}

// MarshalJSON returns the JSON representation of the InputManager.
func (mgr *InputManager) MarshalJSON() ([]byte, error) {
	status := struct {
		Size  base.Byte
		Count int
	}{
		Size:  mgr.lruCache.Size(),
		Count: mgr.lruCache.EntryCount(),
	}

	return json.MarshalIndent(status, "", "  ")
}

// HashReader is a Reader that provides a Sum function. After having completely
// read the Reader, the Sum function will provide the hash of the complete
// stream.
type HashReader struct {
	hasher hash.Hash
	reader io.Reader
	length int64
}

// NewHashReader returns a new HashReader for a given Reader and Hash.
func NewHashReader(r io.Reader, h hash.Hash) *HashReader {
	return &HashReader{
		hasher: h,
		reader: r,
	}
}

// Read calls the underlying Reader's Read function and updates the Hash with
// the read bytes.
func (r *HashReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.length += int64(n)
	r.hasher.Write(b[:n])
	return n, err
}

// drain ensures EOF has been reached in the underlying Reader.
func (r *HashReader) drain() {
	buf := make([]byte, 256)
	for {
		// Read until an error occurs.
		n, err := r.Read(buf)
		r.length += int64(n)
		if err != nil {
			return
		}
	}
}

// Sum returns the hash of the Reader. Typically invoked after Read reaches
// EOF. Will consume any non-consumed data.
func (r *HashReader) Sum(b []byte) []byte {
	r.drain()
	return r.hasher.Sum(b)
}

// Length returns the number of bytes involved in the hash computation so far.
func (r *HashReader) Length() int64 {
	return r.length
}

// Sha1sum is an utility function that obtains the SHA1 hash of a file (as
// referenced to by the filename parameter).
func Sha1sum(filename string) ([]byte, error) {
	hash := sha1.New()
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	if _, err := io.Copy(hash, fd); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}
