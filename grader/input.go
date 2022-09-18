package grader

import (
	"bufio"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/common"
	"github.com/pkg/errors"
)

var (
	slowProblemCache = base.NewLRUCache[slowProblemEntry](4 * 1024 * 1024) // 4 MiB cache should be enough.
)

type slowProblemEntry bool

var _ base.SizedEntry[slowProblemEntry] = (*slowProblemEntry)(nil)

func (e *slowProblemEntry) Size() base.Byte {
	return base.Byte(1)
}

func (e *slowProblemEntry) Release() {
	// It's just, like, a bool.
}

func (e *slowProblemEntry) Value() slowProblemEntry {
	if e == nil {
		return false
	}
	return *e
}

// IsProblemSlow returns whether the problem at that particular commit is slow.
// It uses a global cache to avoid having to ask this question for every single problem.
func IsProblemSlow(
	gitserverURL string,
	gitserverAuthorization string,
	problemName string,
	inputHash string,
) (bool, error) {
	if !strings.HasSuffix(gitserverURL, "/") {
		gitserverURL += "/"
	}
	cacheKey := fmt.Sprintf("%s:%s", problemName, inputHash)
	entry, err := slowProblemCache.Get(cacheKey, func(key string) (base.SizedEntry[slowProblemEntry], error) {
		client := &http.Client{
			Timeout: 15 * time.Second,
		}

		req, err := http.NewRequest("GET", fmt.Sprintf("%s%s/+/%s/settings.json", gitserverURL, problemName, inputHash), nil)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create a request for problem settings for %s", cacheKey)
		}
		if gitserverAuthorization != "" {
			req.Header.Add("Authorization", gitserverAuthorization)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get problem settings for %s", cacheKey)
		}
		var problemSettings common.ProblemSettings
		err = json.NewDecoder(resp.Body).Decode(&problemSettings)
		resp.Body.Close()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal settings.json for %s", cacheKey)
		}

		slow := problemSettings.Slow
		return (*slowProblemEntry)(&slow), nil
	})
	if err != nil {
		return false, err
	}
	slow := *(*bool)(entry.Value.(*slowProblemEntry))
	slowProblemCache.Put(entry)

	return slow, nil
}

// CreateArchiveFromGit creates an archive that can be sent to a Runner as an
// Input from a git repository.
func CreateArchiveFromGit(
	archivePath string,
	gitserverURL string,
	gitserverAuthorization string,
	problemName string,
	inputHash string,
) (int64, error) {
	if !strings.HasSuffix(gitserverURL, "/") {
		gitserverURL += "/"
	}
	client := &http.Client{
		Timeout: 3 * time.Minute,
	}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s/+archive/%s.tar.gz", gitserverURL, problemName, inputHash), nil)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create a request for %s:%s", problemName, inputHash)
	}
	req.Header.Add("If-Tree", inputHash)
	if gitserverAuthorization != "" {
		req.Header.Add("Authorization", gitserverAuthorization)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to do request for %s:%s", problemName, inputHash)
	}

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		resp.Body.Close()
		return 0, errors.Wrapf(err, "failed to create archive for %s:%s", problemName, inputHash)
	}
	defer tmpFd.Close()

	_, err = io.Copy(tmpFd, resp.Body)
	resp.Body.Close()
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read response for %s:%s", problemName, inputHash)
	}

	uncompressedSize, err := strconv.ParseInt(resp.Trailer.Get("Omegaup-Uncompressed-Size"), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get uncompressed size for %s:%s", problemName, inputHash)
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
	gitserverURL           string
	gitserverAuthorization string
	problemName            string
}

// Persist writes the Input to disk and stores its hash.
func (input *Input) Persist() error {
	if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.archivePath)
	defer os.Remove(tmpPath)
	uncompressedSize, err := CreateArchiveFromGit(
		tmpPath,
		input.gitserverURL,
		input.gitserverAuthorization,
		input.problemName,
		input.Hash(),
	)
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

// Value implements the SizedEntry interface.
func (input *Input) Value() common.Input {
	return input
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
		problemName:            factory.problemName,
		gitserverURL:           factory.config.Grader.GitserverURL,
		gitserverAuthorization: factory.config.Grader.GitserverAuthorization,
	}
}

type cachedInput struct {
	graderBaseInput
}

func (input *cachedInput) Persist() error {
	return common.ErrUnimplemented
}

func (input *cachedInput) Value() common.Input {
	return input
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
