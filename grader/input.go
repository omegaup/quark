package grader

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	git "github.com/libgit2/git2go/v33"
	"github.com/omegaup/quark/common"
	"github.com/pkg/errors"
)

// A ProblemInformation represents information from the problem.
type ProblemInformation struct {
	InputHash string
	Settings  *common.ProblemSettings
}

// GetProblemInformation returns the ProblemInformation obtained from the git
// repository located at the provided repository path.
func GetProblemInformation(repositoryPath string) (*ProblemInformation, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open %s", repositoryPath)
	}
	defer repository.Free()
	privateBranchRef, err := repository.References.Lookup("refs/heads/private")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find the private branch for %s", repositoryPath)
	}
	defer privateBranchRef.Free()
	privateBranchObject, err := privateBranchRef.Peel(git.ObjectCommit)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to peel the private branch for %s", repositoryPath)
	}
	defer privateBranchObject.Free()
	privateBranchCommit, err := privateBranchObject.AsCommit()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find private branch commit for %s", repositoryPath)
	}
	defer privateBranchCommit.Free()
	privateBranchTree, err := privateBranchCommit.Tree()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to find private branch commit's tree for %s", repositoryPath)
	}
	defer privateBranchTree.Free()

	settingsJSONEntry := privateBranchTree.EntryByName("settings.json")
	if settingsJSONEntry == nil {
		return nil, errors.Errorf("failed to find settings.json for %s", repositoryPath)
	}
	settingsJSONObject, err := repository.LookupBlob(settingsJSONEntry.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to lookup settings.json for %s", repositoryPath)
	}
	defer settingsJSONObject.Free()
	var problemSettings common.ProblemSettings
	if err := json.Unmarshal(settingsJSONObject.Contents(), &problemSettings); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal settings.json for %s", repositoryPath)
	}

	return &ProblemInformation{
		privateBranchTree.Id().String(),
		&problemSettings,
	}, nil
}

// CreateArchiveFromGit creates an archive that can be sent to a Runner as an
// Input from a git repository.
func CreateArchiveFromGit(
	archivePath string,
	repositoryPath string,
	inputHash string,
) (int64, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open repository for %s:%s", repositoryPath, inputHash)
	}
	defer repository.Free()

	treeOid, err := git.NewOid(inputHash)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse tree hash for %s:%s", repositoryPath, inputHash)
	}
	tree, err := repository.LookupTree(treeOid)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to lookup tree for %s:%s", repositoryPath, inputHash)
	}
	defer tree.Free()
	odb, err := repository.Odb()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get odb for %s:%s", repositoryPath, inputHash)
	}
	defer odb.Free()

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create archive for %s:%s", repositoryPath, inputHash)
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	foundSettings := false
	var uncompressedSize int64
	err = tree.Walk(func(parent string, entry *git.TreeEntry) error {
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
			err := archive.WriteHeader(hdr)
			if err != nil {
				return errors.Wrapf(err, "failed to write header for tree %s", entryPath)
			}

		case git.ObjectBlob:
			blob, err := repository.LookupBlob(entry.Id)
			if err != nil {
				return errors.Wrapf(err, "failed to lookup blob %s", entryPath)
			}
			defer blob.Free()

			hdr := &tar.Header{
				Name:     entryPath,
				Typeflag: tar.TypeReg,
				Mode:     0644,
				Size:     blob.Size(),
			}
			uncompressedSize += blob.Size()
			err = archive.WriteHeader(hdr)
			if err != nil {
				return errors.Wrapf(err, "failed to write header for blob %s", entryPath)
			}

			// Attempt to uncompress this object on the fly from the gzip stream
			// rather than decompressing completely it in memory. This is only
			// possible if the object is not deltified.
			stream, err := odb.NewReadStream(entry.Id)
			if err == nil {
				defer stream.Free()
				_, err = io.Copy(archive, stream)
				if err != nil {
					return errors.Wrapf(err, "failed to copy blob stream %s", entryPath)
				}
			} else {
				// That particular object cannot be streamed. Allocate the blob in
				// memory and write it to the archive.
				_, err = archive.Write(blob.Contents())
				if err != nil {
					return errors.Wrapf(err, "failed to write blob %s", entryPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create archive for %s:%s", repositoryPath, inputHash)
	}
	if !foundSettings {
		return 0, errors.Errorf(
			"Could not find `settings.json` in %s:%s",
			repositoryPath,
			inputHash,
		)
	}
	return uncompressedSize, nil
}

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
		return stderrors.New("Hash verification failed")
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

func (input *graderBaseInput) Release() {
	input.Delete()
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
	uncompressedSize, err := CreateArchiveFromGit(tmpPath, input.repositoryPath, input.Hash())
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

// Transmit sends a serialized version of the Input to the runner. It sends a
// .tar.gz file with the Content-SHA1 header with the hexadecimal
// representation of its SHA-1 hash.
func (input *Input) Transmit(w http.ResponseWriter) error {
	return input.graderBaseInput.Transmit(w)
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
		repositoryPath: GetRepositoryPath(
			factory.config.Grader.RuntimePath,
			factory.problemName,
		),
	}
}

type cachedInput struct {
	graderBaseInput
}

func (input *cachedInput) Persist() error {
	return common.ErrUnimplemented
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
	return &cachedInput{
		graderBaseInput{
			BaseInput: *common.NewBaseInput(
				hash,
				mgr,
			),
			archivePath: path.Join(
				factory.inputPath,
				fmt.Sprintf("%s/%s.tar.gz", hash[:2], hash[2:]),
			),
		},
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

// GetRepositoryPath returns the path of a problem repository.
func GetRepositoryPath(
	root string,
	problemName string,
) string {
	return path.Join(
		root,
		"problems.git",
		problemName,
	)
}
