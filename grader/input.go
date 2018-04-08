package grader

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	git "github.com/libgit2/git2go"
	"github.com/omegaup/quark/common"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

type graderBaseInput struct {
	common.BaseInput
	archivePath      string
	storedHash       string
	uncompressedSize int64
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
	uncompressedSize, err := input.getStoredLength()
	if err != nil {
		return err
	}

	input.storedHash = storedHash
	input.uncompressedSize = uncompressedSize
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

func (input *graderBaseInput) getStoredLength() (int64, error) {
	lenFd, err := os.Open(fmt.Sprintf("%s.len", input.archivePath))
	if err != nil {
		return 0, err
	}
	defer lenFd.Close()
	scanner := bufio.NewScanner(lenFd)
	scanner.Split(bufio.ScanLines)
	if !scanner.Scan() {
		if scanner.Err() != nil {
			return 0, scanner.Err()
		}
		return 0, io.ErrUnexpectedEOF
	}
	return strconv.ParseInt(scanner.Text(), 10, 64)
}

func (input *graderBaseInput) Delete() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
	os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
	os.Remove(fmt.Sprintf("%s.len", input.archivePath))
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
	w.Header().Add(
		"X-Content-Uncompressed-Size", strconv.FormatInt(input.uncompressedSize, 10),
	)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, fd)
	return err
}

// Input is a common.Input generated from a git repository that is then stored
// in a .tar.gz file that can be sent to a runner.
type Input struct {
	graderBaseInput
	repositoryPath string
}

// Persist writes the Input to disk and stores its hash.
func (input *Input) Persist() error {
	if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.archivePath)
	defer os.Remove(tmpPath)
	uncompressedSize, err := input.createArchiveFromGit(tmpPath)
	if err != nil {
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

	sizeFd, err := os.Create(fmt.Sprintf("%s.len", input.archivePath))
	if err != nil {
		return err
	}
	defer sizeFd.Close()

	if _, err := fmt.Fprintf(sizeFd, "%d\n", uncompressedSize); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, input.archivePath); err != nil {
		return err
	}

	input.storedHash = fmt.Sprintf("%0x", hash)
	input.uncompressedSize = uncompressedSize
	input.Commit(stat.Size())
	return nil
}

func (input *Input) createArchiveFromGit(archivePath string) (int64, error) {
	repository, err := git.OpenRepository(input.repositoryPath)
	if err != nil {
		return 0, err
	}
	defer repository.Free()

	treeOid, err := git.NewOid(input.Hash())
	if err != nil {
		return 0, err
	}

	tree, err := repository.LookupTree(treeOid)
	if err != nil {
		return 0, err
	}
	defer tree.Free()
	odb, err := repository.Odb()
	if err != nil {
		return 0, err
	}
	defer odb.Free()

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		return 0, err
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	var walkErr error
	foundSettings := false
	var uncompressedSize int64
	tree.Walk(func(parent string, entry *git.TreeEntry) int {
		entryPath := path.Join(parent, entry.Name)
		if entryPath == "settings.json" {
			foundSettings = true
		}
		switch entry.Type {
		case git.ObjectTree:
			hdr := &tar.Header{
				Name:     entryPath,
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
				Name:     entryPath,
				Typeflag: tar.TypeReg,
				Mode:     0644,
				Size:     blob.Size(),
			}
			uncompressedSize += blob.Size()
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

	if walkErr != nil {
		return 0, walkErr
	}
	if !foundSettings {
		return 0, fmt.Errorf(
			"Could not find `settings.json` in %s:%s",
			input.repositoryPath,
			input.Hash(),
		)
	}
	return uncompressedSize, nil
}

// InputFactory is a common.InputFactory that can store specific versions of a
// problem's git repository into a .tar.gz file that can be easily shipped to
// runners.
type InputFactory struct {
	problemName string
	config      *common.Config
}

// NewInputFactory returns a new InputFactory for the specified problem name
// and configuration.
func NewInputFactory(
	problemName string,
	config *common.Config,
) common.InputFactory {
	return &InputFactory{
		problemName: problemName,
		config:      config,
	}
}

// NewInput creates a new Input that is identified by the supplied hash.
func (factory *InputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &Input{
		graderBaseInput: graderBaseInput{
			BaseInput: *common.NewBaseInput(
				hash,
				mgr,
			),
			archivePath: path.Join(
				factory.config.Grader.RuntimePath,
				"cache",
				fmt.Sprintf("%s/%s.tar.gz", hash[:2], hash[2:]),
			),
		},
		repositoryPath: path.Join(
			factory.config.Grader.RuntimePath,
			"problems.git",
			factory.problemName,
		),
	}
}

// A CachedInputFactory is a grader-specific CachedInputFactory. It reads
// all its inputs from the filesystem, and validates that they have not been
// accidentally corrupted by comparing the input against its hash.
type CachedInputFactory struct {
	inputPath string
}

// NewCachedInputFactory returns a new CachedInputFactory.
func NewCachedInputFactory(inputPath string) common.CachedInputFactory {
	return &CachedInputFactory{
		inputPath: inputPath,
	}
}

// NewInput returns an Input with the provided hash.
func (factory *CachedInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &graderBaseInput{
		BaseInput: *common.NewBaseInput(
			hash,
			mgr,
		),
		archivePath: path.Join(
			factory.inputPath,
			fmt.Sprintf("%s/%s.tar.gz", hash[:2], hash[2:]),
		),
	}
}

// GetInputHash returns the hash of the current InputFactory.
func (factory *CachedInputFactory) GetInputHash(
	dirname string,
	info os.FileInfo,
) (hash string, ok bool) {
	const extension = ".tar.gz"
	filename := path.Base(info.Name())
	if !strings.HasSuffix(filename, extension) {
		return "", false
	}
	return fmt.Sprintf(
		"%s%s",
		path.Base(dirname),
		strings.TrimSuffix(filename, extension),
	), true
}
