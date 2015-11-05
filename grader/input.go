package grader

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/omegaup/quark/common"
	git "gopkg.in/libgit2/git2go.v22"
	"io"
	"net/http"
	"os"
	"path"
)

type GraderInput struct {
	common.BaseInput
	path           string
	repositoryPath string
}

type GraderInputFactory struct {
	run    *common.Run
	config *common.Config
}

func NewGraderInputFactory(run *common.Run, config *common.Config) common.InputFactory {
	return &GraderInputFactory{
		run:    run,
		config: config,
	}
}

func (factory *GraderInputFactory) NewInput(mgr *common.InputManager) common.Input {
	return &GraderInput{
		BaseInput: *common.NewBaseInput(factory.run.InputHash, mgr),
		path: path.Join(factory.config.Grader.RuntimePath,
			"cache", fmt.Sprintf("%s.tar.gz", factory.run.InputHash)),
		repositoryPath: path.Join(factory.config.Grader.RuntimePath,
			"problems.git", factory.run.Problem.Name),
	}
}

func (input *GraderInput) Transmit(w http.ResponseWriter) error {
	hash, err := input.getStoredHash()
	if err != nil {
		return err
	}
	fd, err := os.Open(input.path)
	if err != nil {
		return err
	}
	defer fd.Close()
	w.Header().Add("Content-SHA1", hash)
	fmt.Println(hash)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, fd)
	return err
}

func (input *GraderInput) getStoredHash() (string, error) {
	hashFd, err := os.Open(fmt.Sprintf("%s.sha1", input.path))
	if err != nil {
		return "", err
	}
	defer hashFd.Close()
	scanner := bufio.NewScanner(hashFd)
	scanner.Split(bufio.ScanWords)
	if !scanner.Scan() {
		if scanner.Err() != nil {
			return "", scanner.Err()
		}
		return "", io.ErrUnexpectedEOF
	}
	return scanner.Text(), nil
}

func (input *GraderInput) Verify() error {
	stat, err := os.Stat(input.path)
	if err != nil {
		return err
	}
	hash, err := common.Sha1sum(input.path)
	if err != nil {
		return err
	}
	storedHash, err := input.getStoredHash()
	if err != nil {
		return err
	}
	if storedHash != fmt.Sprintf("%0x", hash) {
		return errors.New("Hash verification failed")
	}

	input.Commit(stat.Size())
	return nil
}

func (input *GraderInput) CreateArchive() error {
	if err := os.MkdirAll(path.Dir(input.path), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.path)
	defer os.Remove(tmpPath)
	if err := input.createArchiveFromGit(tmpPath); err != nil {
		return err
	}

	stat, err := os.Stat(tmpPath)
	if err != nil {
		return err
	}

	hash, err := common.Sha1sum(tmpPath)
	if err != nil {
		return err
	}

	hashFd, err := os.Create(fmt.Sprintf("%s.sha1", input.path))
	if err != nil {
		return err
	}
	defer hashFd.Close()

	if _, err := fmt.Fprintf(hashFd, "%0x *%s\n", hash, path.Base(input.path)); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, input.path); err != nil {
		return err
	}

	input.Commit(stat.Size())
	return nil
}

func (input *GraderInput) DeleteArchive() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.path))
	os.Remove(fmt.Sprintf("%s.sha1", input.path))
	return os.Remove(input.path)
}

func (input *GraderInput) createArchiveFromGit(archivePath string) error {
	repository, err := git.OpenRepository(input.repositoryPath)
	if err != nil {
		return err
	}
	defer repository.Free()

	treeOid, err := git.NewOid(input.Hash())
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
				Name:     path.Join(parent, entry.Name),
				Typeflag: tar.TypeDir,
				Mode:     0755,
				Size:     0,
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
				Name:     path.Join(parent, entry.Name),
				Typeflag: tar.TypeReg,
				Mode:     0644,
				Size:     blob.Size(),
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
