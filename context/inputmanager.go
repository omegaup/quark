package context

import (
	"container/list"
	"crypto/sha1"
	"errors"
	"expvar"
	"fmt"
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
	Hash() string
	Committed() bool
	Size() int64
	Verify() error
	CreateArchive() error
	DeleteArchive() error
}

type InputFactory interface {
	NewInput(mgr *InputManager) Input
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

func (mgr *InputManager) Get(hash string, factory InputFactory) (Input, error) {
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
			mgr.ctx.Log.Error("Hash verification failed. Regenerating",
				"path", input.Hash(), "err", err)
			input.DeleteArchive()

			if err := input.CreateArchive(); err != nil {
				mgr.ctx.Log.Error("Error creating archive", "hash", input.Hash())
				return nil, err
			}
			mgr.ctx.Log.Info("Generated input", "hash", input.Hash())
		} else {
			mgr.ctx.Log.Info("Reusing input", "hash", input.Hash())
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

func sha1sum(path string) ([]byte, error) {
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
