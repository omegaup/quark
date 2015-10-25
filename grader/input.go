package grader

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/omegaup/quark/context"
	git "gopkg.in/libgit2/git2go.v22"
	"io"
	"os"
	"path"
	"sync"
)

type GraderInput struct {
	sync.Mutex
	committed      bool
	size           int64
	hash           string
	path           string
	repositoryPath string
	refcount       int
	mgr            *context.InputManager
}

type GraderInputFactory struct {
	run    *Run
	config *context.Config
}

func NewGraderInputFactory(run *Run, config *context.Config) context.InputFactory {
	return &GraderInputFactory{
		run:    run,
		config: config,
	}
}

func (factory *GraderInputFactory) NewInput(mgr *context.InputManager) context.Input {
	return &GraderInput{
		mgr:            mgr,
		hash:           factory.run.InputHash,
		path:           factory.run.GetInputPath(factory.config),
		repositoryPath: factory.run.GetRepositoryPath(factory.config),
	}
}

func (input *GraderInput) Committed() bool {
	return input.committed
}

func (input *GraderInput) Size() int64 {
	return input.size
}

func (input *GraderInput) Hash() string {
	return input.hash
}

func (input *GraderInput) Verify() error {
	stat, err := os.Stat(input.path)
	if err != nil {
		return err
	}
	hash, err := sha1sum(input.path)
	if err != nil {
		return err
	}
	hashFd, err := os.Open(fmt.Sprintf("%s.sha1", input.path))
	if err != nil {
		return err
	}
	defer hashFd.Close()
	scanner := bufio.NewScanner(hashFd)
	scanner.Split(bufio.ScanWords)
	if !scanner.Scan() {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		return io.ErrUnexpectedEOF
	}
	if scanner.Text() != fmt.Sprintf("%0x", hash) {
		return errors.New("Hash verification failed")
	}

	input.commit(stat.Size())
	return nil
}

func (input *GraderInput) Acquire() {
	input.refcount++
}

func (input *GraderInput) Release() {
	input.Lock()
	defer input.Unlock()

	input.refcount--
	if input.refcount == 0 {
		// There are no outstanding references to this input. Return it to the
		// input manager where it can be deleted if we need more space.
		input.mgr.Insert(input)
	}
}

func (input *GraderInput) CreateArchive() error {
	tmpPath := fmt.Sprintf("%s.tmp", input.path)
	defer os.Remove(tmpPath)
	if err := input.createArchiveFromGit(tmpPath); err != nil {
		return err
	}

	stat, err := os.Stat(tmpPath)
	if err != nil {
		return err
	}

	hash, err := sha1sum(tmpPath)
	if err != nil {
		return err
	}

	hashFd, err := os.Create(fmt.Sprintf("%s.sha1", input.path))
	if err != nil {
		return err
	}
	defer hashFd.Close()

	if _, err := fmt.Fprintf(hashFd, "%0x *%s", hash, path.Base(input.path)); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, input.path); err != nil {
		return err
	}

	input.commit(stat.Size())
	return nil
}

func (input *GraderInput) DeleteArchive() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.path))
	os.Remove(fmt.Sprintf("%s.sha1", input.path))
	return os.Remove(input.path)
}

func (input *GraderInput) commit(size int64) {
	input.size = size
	input.committed = true
}

func (input *GraderInput) createArchiveFromGit(archivePath string) error {
	repository, err := git.OpenRepository(input.repositoryPath)
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

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
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
