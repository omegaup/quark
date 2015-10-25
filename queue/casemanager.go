package queue

import (
	"archive/tar"
	"compress/gzip"
	"container/list"
	"expvar"
	"fmt"
	"github.com/omegaup/quark/context"
	git "gopkg.in/libgit2/git2go.v22"
	"io"
	"os"
	"path"
	"sync"
)

var (
	DefaultCodeManager *CodeManager
)

type CodeManager struct {
	sync.Mutex
	mapping   map[string]*Input
	evictList *list.List
	ctx       *context.Context
	totalSize int64
	sizeLimit int64
}

type Input struct {
	sync.Mutex
	size        int64
	hash        string
	path        string
	committed   bool
	refcount    int
	mgr         *CodeManager
	listElement *list.Element
}

func InitCodeManager(ctx *context.Context) {
	DefaultCodeManager = &CodeManager{
		mapping:   make(map[string]*Input),
		evictList: list.New(),
		ctx:       ctx,
		sizeLimit: ctx.Config.Grader.CacheSize,
	}
	expvar.Publish("codemanager_size", expvar.Func(func() interface{} {
		return DefaultCodeManager.totalSize
	}))
}

func (mgr *CodeManager) getInput(hash string) *Input {
	mgr.Lock()
	defer mgr.Unlock()

	if ent, ok := mgr.mapping[hash]; ok {
		return ent
	}

	input := &Input{
		mgr:  mgr,
		hash: hash,
	}
	mgr.mapping[hash] = input
	return input
}

func (mgr *CodeManager) Get(run *Run) (*Input, error) {
	input := mgr.getInput(run.Problem.Hash)

	input.Lock()
	defer input.Unlock()

	if input.listElement != nil {
		mgr.totalSize -= input.size
		mgr.evictList.Remove(input.listElement)
		input.listElement = nil
	}

	if !input.committed {
		// This operation can take a while.
		input.path = run.GetInputPath(&mgr.ctx.Config)
		err := mgr.cacheInput(run.GetRepositoryPath(&mgr.ctx.Config), input)
		if err != nil {
			return nil, err
		}

		input.committed = true
	}
	input.refcount++
	return input, nil
}

func (mgr *CodeManager) cacheInput(repositoryPath string, input *Input) error {
	stat, err := os.Stat(input.path)
	if err == nil {
		// File already exists.
		input.size = stat.Size()
		return nil
	}

	if err := input.createArchive(repositoryPath); err != nil {
		return err
	}

	stat, err = os.Stat(input.path)
	if err != nil {
		return err
	}
	input.size = stat.Size()
	return nil
}

func (input *Input) Release() {
	input.Lock()
	defer input.Unlock()

	input.refcount--
	if input.refcount != 0 {
		// There is at least one other reference to this input. Let it live.
		return
	}

	mgr := input.mgr
	mgr.Lock()
	defer mgr.Unlock()

	mgr.totalSize += input.size
	input.listElement = mgr.evictList.PushFront(input)
	// Evict elements as necessary to get below the allowed limit.
	for mgr.evictList.Len() > 0 && mgr.totalSize > mgr.sizeLimit {
		fmt.Printf("Evicting a stuff. %d > %d\n", mgr.totalSize, mgr.sizeLimit)
		element := mgr.evictList.Back()
		input := element.Value.(*Input)

		mgr.totalSize -= input.size
		mgr.evictList.Remove(element)

		delete(mgr.mapping, input.hash)
		input.deleteArchive()
		fmt.Printf("Released %d bytes, now at %d\n", input.size, mgr.totalSize)
	}
}

func (input *Input) createArchive(repositoryPath string) error {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return err
	}
	defer repository.Free()

	treeOid, err := git.NewOid(input.hash)
	if err != nil {
		return err
	}

	tree, err := repository.LookupTree(treeOid)
	if err != nil {
		return err
	}
	defer tree.Free()
	odb, err := repository.Odb()
	if err != nil {
		return err
	}
	defer odb.Free()

	f, err := os.Create(input.path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	var walkErr error = nil
	tree.Walk(func(parent string, entry *git.TreeEntry) int {
		switch entry.Type {
		case git.ObjectTree:
			hdr := &tar.Header{
				Name: path.Join(parent, entry.Name),
				Mode: 0755,
				Size: 0,
			}
			if walkErr = archive.WriteHeader(hdr); walkErr != nil {
				return -1
			}
		case git.ObjectBlob:
			blob, walkErr := repository.LookupBlob(entry.Id)
			if walkErr != nil {
				return -1
			}
			defer blob.Free()

			hdr := &tar.Header{
				Name: path.Join(parent, entry.Name),
				Mode: 0755,
				Size: blob.Size(),
			}
			if walkErr = archive.WriteHeader(hdr); walkErr != nil {
				return -1
			}

			stream, err := odb.NewReadStream(entry.Id)
			if err == nil {
				defer stream.Free()
				if _, walkErr := io.Copy(archive, stream); walkErr != nil {
					return -1
				}
			} else {
				// That particular object cannot be streamed. Allocate the blob in
				// memory and write it to the archive.
				if _, walkErr := archive.Write(blob.Contents()); walkErr != nil {
					return -1
				}
			}
		}
		return 0
	})

	return walkErr
}

func (input *Input) deleteArchive() error {
	return os.Remove(input.path)
}
