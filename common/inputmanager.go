package common

import (
	"container/list"
	"crypto/sha1"
	"errors"
	"expvar"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
)

type InputManager struct {
	sync.Mutex
	mapping   map[string]*cacheEntry
	evictList *list.List
	ctx       *Context
	totalSize int64
	sizeLimit int64
}

type cacheEntry struct {
	input       Input
	listElement *list.Element
}

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
	Release()
}

type Input interface {
	Lockable
	RefCounted
	Path() string
	Hash() string
	Committed() bool
	Size() int64
	Verify() error
	CreateArchive() error
	DeleteArchive() error
	Settings() *ProblemSettings
}

type BaseInput struct {
	sync.Mutex
	committed bool
	refcount  int
	size      int64
	path      string
	hash      string
	mgr       *InputManager
	settings  ProblemSettings
}

type InputFactory interface {
	NewInput(mgr *InputManager) Input
}

type HashReader struct {
	hasher hash.Hash
	reader io.Reader
}

func InitInputManager(ctx *Context) {
	DefaultInputManager = &InputManager{
		mapping:   make(map[string]*cacheEntry),
		evictList: list.New(),
		ctx:       ctx,
		sizeLimit: ctx.Config.Grader.CacheSize,
	}
	expvar.Publish("codemanager_size", expvar.Func(func() interface{} {
		return DefaultInputManager.totalSize
	}))
}

func (mgr *InputManager) getEntry(hash string, factory InputFactory) *cacheEntry {
	mgr.Lock()
	defer mgr.Unlock()

	return mgr.getEntryLocked(hash, factory)
}

func (mgr *InputManager) getEntryLocked(hash string, factory InputFactory) *cacheEntry {
	if ent, ok := mgr.mapping[hash]; ok {
		return ent
	}

	if factory == nil {
		panic(errors.New(fmt.Sprintf("hash %s not found and factory is nil", hash)))
	}

	input := factory.NewInput(mgr)
	entry := &cacheEntry{
		input: input,
	}
	mgr.mapping[hash] = entry
	return entry
}

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
			input.DeleteArchive()

			if err := input.CreateArchive(); err != nil {
				mgr.ctx.Log.Error("Error creating archive", "hash", input.Hash())
				delete(mgr.mapping, hash)
				return nil, err
			}
			mgr.ctx.Log.Info("Generated input", "hash", input.Hash())
		} else {
			mgr.ctx.Log.Debug("Reusing input", "hash", input.Hash())
		}
	}
	input.Acquire()
	return input, nil
}

func (mgr *InputManager) Insert(input Input) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.totalSize += input.Size()
	entry := mgr.getEntryLocked(input.Hash(), nil)
	entry.listElement = mgr.evictList.PushFront(input)

	// Evict elements as necessary to get below the allowed limit.
	for mgr.evictList.Len() > 0 && mgr.totalSize > mgr.sizeLimit {
		mgr.ctx.Log.Info("Evicting an input", "input", input, "size", input.Size())
		element := mgr.evictList.Back()
		input := element.Value.(Input)

		mgr.totalSize -= input.Size()
		mgr.evictList.Remove(element)

		delete(mgr.mapping, input.Hash())
		input.DeleteArchive()
	}
}

func (mgr *InputManager) Size() int64 {
	return mgr.totalSize
}

func NewBaseInput(hash string, mgr *InputManager, path string) *BaseInput {
	return &BaseInput{
		hash: hash,
		mgr:  mgr,
		path: path,
	}
}

func (input *BaseInput) Committed() bool {
	return input.committed
}

func (input *BaseInput) Size() int64 {
	return input.size
}

func (input *BaseInput) Path() string {
	return input.path
}

func (input *BaseInput) Hash() string {
	return input.hash
}

func (input *BaseInput) Commit(size int64) {
	input.size = size
	input.committed = true
}

func (input *BaseInput) Verify() error {
	return errors.New("Unimplemented")
}

func (input *BaseInput) CreateArchive() error {
	return errors.New("Unimplemented")
}

func (input *BaseInput) DeleteArchive() error {
	return nil
}

func (input *BaseInput) Settings() *ProblemSettings {
	return &input.settings
}

func (input *BaseInput) Acquire() {
	input.refcount++
}

func (input *BaseInput) Release() {
	input.Lock()
	defer input.Unlock()

	input.refcount--
	if input.refcount == 0 {
		// There are no outstanding references to this input. Return it to the
		// input manager where it can be deleted if we need more space.
		input.mgr.Insert(input)
	}
}

func NewHashReader(r io.Reader, h hash.Hash) *HashReader {
	return &HashReader{
		hasher: h,
		reader: r,
	}
}

func (r *HashReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	r.hasher.Write(b[:n])
	return n, err
}

func (r *HashReader) Sum(b []byte) []byte {
	return r.hasher.Sum(b)
}

func Sha1sum(path string) ([]byte, error) {
	hash := sha1.New()
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	if _, err := io.Copy(hash, fd); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}
