package grader

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	git "gopkg.in/libgit2/git2go.v22"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
)

type graderBaseInput struct {
	common.BaseInput
	archivePath string
	storedHash  string
}

func (input *graderBaseInput) Verify() error {
	stat, err := os.Stat(input.archivePath)
	if err != nil {
		return err
	}
	hash, err := common.Sha1sum(input.archivePath)
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

	input.storedHash = storedHash
	input.Commit(stat.Size())
	return nil
}

func (input *graderBaseInput) getStoredHash() (string, error) {
	hashFd, err := os.Open(fmt.Sprintf("%s.sha1", input.archivePath))
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

func (input *graderBaseInput) Delete() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
	os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
	return os.Remove(input.archivePath)
}

// Transmit sends a serialized version of the Input to the runner. It sends a
// .tar.gz file with the Content-SHA1 header with the hexadecimal
// representation of its SHA-1 hash.
func (input *graderBaseInput) Transmit(w http.ResponseWriter) error {
	fd, err := os.Open(input.archivePath)
	if err != nil {
		return err
	}
	defer fd.Close()
	w.Header().Add("Content-Type", "application/x-gzip")
	w.Header().Add("Content-SHA1", input.storedHash)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, fd)
	return err
}

// GraderInput is an Input generated from a git repository that is then stored
// in a .tar.gz file that can be sent to a runner.
type GraderInput struct {
	graderBaseInput
	repositoryPath string
}

func (input *GraderInput) Persist() error {
	if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.archivePath)
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

	hashFd, err := os.Create(fmt.Sprintf("%s.sha1", input.archivePath))
	if err != nil {
		return err
	}
	defer hashFd.Close()

	if _, err := fmt.Fprintf(
		hashFd,
		"%0x *%s\n",
		hash,
		path.Base(input.archivePath),
	); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, input.archivePath); err != nil {
		return err
	}

	input.storedHash = fmt.Sprintf("%0x", hash)
	input.Commit(stat.Size())
	return nil
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

// GraderInputFactory is an InputFactory that can store specific versions of a
// problem's git repository into a .tar.gz file that can be easily shipped to
// runners.
type GraderInputFactory struct {
	problemName string
	config      *common.Config
}

func NewGraderInputFactory(
	problemName string,
	config *common.Config,
) common.InputFactory {
	return &GraderInputFactory{
		problemName: problemName,
		config:      config,
	}
}

func (factory *GraderInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &GraderInput{
		graderBaseInput: graderBaseInput{
			BaseInput: *common.NewBaseInput(
				hash,
				mgr,
			),
			archivePath: path.Join(
				factory.config.Grader.RuntimePath,
				"cache",
				fmt.Sprintf("%s.tar.gz", hash),
			),
		},
		repositoryPath: path.Join(
			factory.config.Grader.RuntimePath,
			"problems.git",
			factory.problemName,
		),
	}
}

// GraderCachedInputFactory is a grader-specific CachedInputFactory.
type GraderCachedInputFactory struct {
	inputPath string
}

func NewGraderCachedInputFactory(inputPath string) common.CachedInputFactory {
	return &GraderCachedInputFactory{
		inputPath: inputPath,
	}
}

func (factory *GraderCachedInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &graderBaseInput{
		BaseInput: *common.NewBaseInput(
			hash,
			mgr,
		),
		archivePath: path.Join(factory.inputPath, fmt.Sprintf("%s.tar.gz", hash)),
	}
}

func (factory *GraderCachedInputFactory) GetInputHash(
	info os.FileInfo,
) (hash string, ok bool) {
	const extension = ".tar.gz"
	filename := path.Base(info.Name())
	if !strings.HasSuffix(filename, extension) {
		return "", false
	}
	return strings.TrimSuffix(filename, extension), true
}
