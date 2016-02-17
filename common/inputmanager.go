package common

import (
	"container/list"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sync"
)

// Lockable is the interface that sync.Mutex implements.
type Lockable interface {
	Lock()
	Unlock()
}

// RefCounted is the interface that provides reference-counted semantics.
//
// Once the final reference has been released, the object is expected to
// release all resources.
type RefCounted interface {
	Acquire()

	// Unfortunately, Go always invokes the methods of an embedded type with the
	// embedded type as the receiver (rather than the outer type), effectively
	// upcasting the pointer and losing the actual type. In order to avoid that,
	// we need to pass the Input again as a parameter.
	Release(Input)
}

// Input represents a problem's input set.
//
// Input is reference-counted, so it will keep the input in memory (and disk)
// while there is at least one reference to it. Once the last reference is
// released, it will be inserted into its associated InputManager.
type Input interface {
	Lockable
	RefCounted

	// Path returns the path to the uncompressed representation of the Input
	// on-disk.
	Path() string

	// Hash is the identifier of the Input. It is typically the Git hash of the
	// tree that it represents.
	Hash() string

	// Commited returns true if the input has been verified and is committed into
	// its InputManager.
	Committed() bool

	// Size returns the number of bytes of the Input's on-disk representation.
	Size() int64

	// Verify ensures that the version of the Input stored in the filesystem
	// exists and is consistent, and commits it so that it can be added to the
	// InputManager. It is expected that Size() returns a valid size after
	// Verify() returns successfully.
	Verify() error

	// Persist stores the Input into the filesystem in a form that is
	// easily consumed and can be verified for consistency after restarting.
	Persist() error

	// Delete removes the filesystem version of the Input to free space.
	Delete() error

	// Settings returns the problem's settings for the Input. Problems can have
	// different ProblemSettings on different Inputs.
	Settings() *ProblemSettings

	// Transmit sends a serialized version of the Input over HTTP. It should be a
	// .tar.gz file with the Content-SHA1 header set to the hexadecimal
	// representation of the SHA-1 hash of the file.
	Transmit(http.ResponseWriter) error
}

// BaseInput is an abstract struct that provides most of the functions required
// to implement Input.
type BaseInput struct {
	sync.Mutex
	committed bool
	refcount  int
	size      int64
	hash      string
	mgr       *InputManager
	settings  ProblemSettings
}

func NewBaseInput(hash string, mgr *InputManager) *BaseInput {
	return &BaseInput{
		hash: hash,
		mgr:  mgr,
	}
}

func (input *BaseInput) Path() string {
	return "/dev/null"
}

func (input *BaseInput) Committed() bool {
	return input.committed
}

func (input *BaseInput) Size() int64 {
	return input.size
}

func (input *BaseInput) Hash() string {
	return input.hash
}

func (input *BaseInput) Reserve(size int64) {
	input.mgr.Reserve(size)
}

func (input *BaseInput) Commit(size int64) {
	input.size = size
	input.committed = true
}

func (input *BaseInput) Verify() error {
	return errors.New("Unimplemented")
}

func (input *BaseInput) Persist() error {
	return errors.New("Unimplemented")
}

func (input *BaseInput) Delete() error {
	return nil
}

func (input *BaseInput) Settings() *ProblemSettings {
	return &input.settings
}

func (input *BaseInput) Acquire() {
	input.refcount++
}

func (input *BaseInput) Release(outerInput Input) {
	input.Lock()
	defer input.Unlock()

	input.refcount--
	if input.refcount == 0 {
		// There are no outstanding references to this input. Return it to the
		// input manager where it can be deleted if we need more space.
		input.mgr.Insert(outerInput)
	}
}

func (input *BaseInput) Transmit(w http.ResponseWriter) error {
	return errors.New("Unimplemented")
}

// InputManager handles a pool of recently-used input sets. The pool has a
// fixed maximum size with a least-recently used eviction policy.
type InputManager struct {
	sync.Mutex
	mapping   map[string]*inputEntry
	evictList *list.List
	ctx       *Context
	totalSize int64
	sizeLimit int64
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

// NewInputManager creates a new InputManager with the provided Context.
func NewInputManager(ctx *Context) *InputManager {
	return &InputManager{
		mapping:   make(map[string]*inputEntry),
		evictList: list.New(),
		ctx:       ctx,
		sizeLimit: ctx.Config.InputManager.CacheSize,
	}
}

func (mgr *InputManager) getEntry(hash string, factory InputFactory) *inputEntry {
	mgr.Lock()
	defer mgr.Unlock()

	return mgr.getEntryLocked(hash, factory)
}

func (mgr *InputManager) getEntryLocked(
	hash string,
	factory InputFactory,
) *inputEntry {
	if ent, ok := mgr.mapping[hash]; ok {
		return ent
	}

	if factory == nil {
		panic(errors.New(fmt.Sprintf("hash %s not found and factory is nil", hash)))
	}

	input := factory.NewInput(hash, mgr)
	if input == nil {
		panic(errors.New(fmt.Sprintf("input nil for hash %s", hash)))
	}
	entry := &inputEntry{
		input: input,
	}
	mgr.mapping[hash] = entry
	return entry
}

// Get returns the Input for a specified hash, if it is already present in the
// pool. If it is present, it will increment its reference-count and transfer
// the ownership of the reference to the caller. It is the caller's
// responsibility to release the ownership of the Input.
func (mgr *InputManager) Get(hash string) (Input, error) {
	mgr.Lock()
	defer mgr.Unlock()

	if ent, ok := mgr.mapping[hash]; ok {
		ent.input.Lock()
		defer ent.input.Unlock()

		ent.input.Acquire()
		return ent.input, nil
	}
	return nil, errors.New(fmt.Sprintf("hash %s not found", hash))
}

// Add associates an opaque identifier (the hash) with an Input in the
// InputManager.
//
// The InputFactory is responsible to create the Input if it has not yet been
// created. The Input will be validated if it has not been committed to the
// InputManager, but will still not be accounted into the size limit since
// there is at least one live reference to it.
func (mgr *InputManager) Add(hash string, factory InputFactory) (Input, error) {
	entry := mgr.getEntry(hash, factory)
	input := entry.input

	input.Lock()
	defer input.Unlock()

	if entry.listElement != nil {
		// This Input did not have any outstanding references and was in the
		// evict list. Remove from it so it does not get accidentally evicted.
		mgr.totalSize -= input.Size()
		mgr.evictList.Remove(entry.listElement)
		entry.listElement = nil
	}

	if !input.Committed() {
		// This operation can take a while.
		if err := input.Verify(); err != nil {
			mgr.ctx.Log.Warn("Hash verification failed. Regenerating",
				"path", input.Hash(), "err", err)
			input.Delete()

			if err := input.Persist(); err != nil {
				mgr.ctx.Log.Error(
					"Error creating archive",
					"hash", input.Hash(),
					"err", err,
				)
				delete(mgr.mapping, hash)
				return nil, err
			}
			mgr.ctx.Log.Info("Generated input", "hash", input.Hash(), "size", input.Size())
		} else {
			mgr.ctx.Log.Debug("Reusing input", "hash", input.Hash(), "size", input.Size())
		}
	}
	input.Acquire()
	return input, nil
}

// Insert adds an already-created Input to the pool, possibly evicting old
// entries to free enough space such that the total size of Inputs in the pool
// is still within the limit. This should only be called when there are no
// references to the Input.
func (mgr *InputManager) Insert(input Input) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.totalSize += input.Size()
	entry := mgr.getEntryLocked(input.Hash(), NewIdentityInputFactory(input))
	entry.listElement = mgr.evictList.PushFront(input)

	// After inserting the input, trim the cache.
	mgr.reserveLocked(0)
}

// Reserve evicts Inputs from the pool to make the specified size available.
func (mgr *InputManager) Reserve(size int64) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.reserveLocked(size)
}

// reserveLocked evicts elements as necessary so that the current commited size
// plus the specified size is below the allowed limit.
func (mgr *InputManager) reserveLocked(size int64) {
	for mgr.evictList.Len() > 0 && mgr.totalSize+size > mgr.sizeLimit {
		element := mgr.evictList.Back()
		evictedInput := element.Value.(Input)

		mgr.totalSize -= evictedInput.Size()
		mgr.evictList.Remove(element)

		delete(mgr.mapping, evictedInput.Hash())
		mgr.ctx.Log.Info(
			"Evicting an input",
			"input", evictedInput,
			"size", evictedInput.Size(),
			"err", evictedInput.Delete(),
		)
	}
}

// Size returns the total size (in bytes) of cached Inputs in the InputManager.
// This does not count any Inputs with live references.
func (mgr *InputManager) Size() int64 {
	return mgr.totalSize
}

// PreloadInputs reads all files in path, runs them through the specified
// filter, and tries to add them into the InputManager. PreloadInputs acquires
// the ioLock just before doing I/O in order to guarantee that the system will
// not be doing expensive I/O operations in the middle of a
// performance-sensitive operation (like running contestants' code).
func (mgr *InputManager) PreloadInputs(
	rootdir string,
	factory CachedInputFactory,
	ioLock *sync.Mutex,
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
			input, err := mgr.Add(hash, factory)
			if err != nil {
				os.RemoveAll(path.Join(dirname, info.Name()))
				mgr.ctx.Log.Error("Cached input corrupted", "hash", hash)
			} else {
				input.Release(input)
			}
			ioLock.Unlock()
		}
	}
	mgr.ctx.Log.Info("Finished preloading cached inputs",
		"cache_size", mgr.Size())
	return nil
}

func (mgr *InputManager) MarshalJSON() ([]byte, error) {
	mgr.Lock()
	defer mgr.Unlock()

	status := struct {
		Size  int64
		Count int
	}{
		Size:  mgr.Size(),
		Count: len(mgr.mapping),
	}

	return json.MarshalIndent(status, "", "  ")
}

// HashReader is a Reader that provides a Sum function. After having completely
// read the Reader, the Sum function will provide the hash of the complete
// stream.
type HashReader struct {
	hasher hash.Hash
	reader io.Reader
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
	r.hasher.Write(b[:n])
	return n, err
}

// Ensures EOF has been reached in the underlying Reader.
func (r *HashReader) Drain() {
	buf := make([]byte, 256)
	for {
		// Read until an error occurs.
		if _, err := r.Read(buf); err != nil {
			return
		}
	}
}

// Sum returns the hash of the Reader. Typically invoked after Read reaches
// EOF.
func (r *HashReader) Sum(b []byte) []byte {
	return r.hasher.Sum(b)
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
