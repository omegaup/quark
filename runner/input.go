package runner

import (
	"archive/tar"
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/omegaup/quark/common"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// InputFactory is a common.InputFactory that can fetch the test case data from
// the grader.
type InputFactory struct {
	client  *http.Client
	config  *common.Config
	baseURL *url.URL
}

// NewInputFactory returns a new InputFactory.
func NewInputFactory(
	client *http.Client,
	config *common.Config,
	baseURL *url.URL,
) common.InputFactory {
	return &InputFactory{
		client:  client,
		config:  config,
		baseURL: baseURL,
	}
}

// NewInput returns a new Input that corresponds to the specified hash.
func (factory *InputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	requestURL, err := factory.baseURL.Parse(fmt.Sprintf("input/%s/", hash))
	if err != nil {
		panic(err)
	}
	return &Input{
		runnerBaseInput: runnerBaseInput{
			BaseInput: *common.NewBaseInput(
				hash,
				mgr,
			),
			path: path.Join(
				factory.config.Runner.RuntimePath,
				"input",
				fmt.Sprintf("%s/%s", hash[:2], hash[2:]),
			),
		},
		client:     factory.client,
		requestURL: requestURL.String(),
	}
}

type runnerBaseInput struct {
	common.BaseInput
	path string
}

func (input *runnerBaseInput) Path() string {
	return input.path
}

func (input *runnerBaseInput) Verify() error {
	hashes, err := input.getStoredHashes()
	if err != nil {
		return err
	}

	var size int64
	for path, expectedHashStr := range hashes {
		actualHash, err := common.Sha1sum(path)
		if err != nil {
			return err
		}
		actualHashStr := fmt.Sprintf("%0x", actualHash)
		if actualHashStr != expectedHashStr {
			return fmt.Errorf(
				"hash mismatch for '%s' == %q, want %q",
				path,
				actualHashStr,
				expectedHashStr,
			)
		}
		stat, err := os.Stat(path)
		if err != nil {
			return err
		}
		size += stat.Size()
	}

	settingsFd, err := os.Open(path.Join(input.path, "settings.json"))
	if err != nil {
		return err
	}
	defer settingsFd.Close()
	decoder := json.NewDecoder(settingsFd)
	if err := decoder.Decode(input.Settings()); err != nil {
		return err
	}

	input.Commit(size)
	return nil
}

func (input *runnerBaseInput) Delete() error {
	os.RemoveAll(fmt.Sprintf("%s.tmp", input.path))
	os.RemoveAll(fmt.Sprintf("%s.sha1", input.path))
	return os.RemoveAll(input.path)
}

func (input *runnerBaseInput) getStoredHashes() (map[string]string, error) {
	result := make(map[string]string)
	dir := filepath.Dir(input.path)
	hashPath := fmt.Sprintf("%s.sha1", input.path)
	hashFd, err := os.Open(hashPath)
	if err != nil {
		return result, err
	}
	defer hashFd.Close()
	scanner := bufio.NewScanner(hashFd)
	scanner.Split(bufio.ScanLines)
	sha1sumRe := regexp.MustCompile("^\\s*([a-f0-9]{40}) [ *](.+)\\s*$")
	for scanner.Scan() {
		res := sha1sumRe.FindStringSubmatch(scanner.Text())
		if res == nil {
			return result, fmt.Errorf(
				"sha1sum file format error '%s",
				hashPath,
			)
		}
		expectedHash := res[1]
		filePath := filepath.Join(dir, res[2])
		if !strings.HasPrefix(filePath, input.path) {
			return result, fmt.Errorf(
				"path is outside expected directory: '%s",
				filePath,
			)
		}
		result[filePath] = expectedHash
	}
	if scanner.Err() != nil {
		return result, scanner.Err()
	}
	return result, nil
}

func (input *runnerBaseInput) persistFromTarStream(
	r io.Reader,
	compressionFormat string,
	uncompressedSize int64,
	streamHash string,
) error {
	tmpPath := fmt.Sprintf("%s.tmp", input.path)
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpPath)

	input.Reserve(uncompressedSize)

	hasher := common.NewHashReader(r, sha1.New())
	var uncompressedReader io.Reader

	if compressionFormat == "gzip" {
		gz, err := gzip.NewReader(hasher)
		if err != nil {
			return err
		}
		defer gz.Close()
		uncompressedReader = gz
	} else if compressionFormat == "bzip2" {
		uncompressedReader = bzip2.NewReader(hasher)
	} else {
		uncompressedReader = hasher
	}

	archive := tar.NewReader(uncompressedReader)

	sha1sumFile, err := os.Create(fmt.Sprintf("%s.sha1", input.path))
	if err != nil {
		return err
	}
	defer sha1sumFile.Close()

	var size int64

	for {
		hdr, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		filePath := path.Join(tmpPath, hdr.Name)
		if hdr.FileInfo().IsDir() {
			if err := os.MkdirAll(filePath, 0755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
			return err
		}
		fd, err := os.Create(filePath)
		if err != nil {
			return err
		}

		innerHasher := common.NewHashReader(archive, sha1.New())
		_, err = io.Copy(fd, innerHasher)
		fd.Close()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(
			sha1sumFile,
			"%0x *%s/%s\n",
			innerHasher.Sum(nil),
			input.Hash()[2:],
			hdr.Name,
		)
		if err != nil {
			return err
		}
		size += hdr.Size
	}

	if streamHash != fmt.Sprintf("%0x", hasher.Sum(nil)) {
		return fmt.Errorf(
			"hash mismatch: expected %s got %s",
			streamHash,
			fmt.Sprintf("%0x", hasher.Sum(nil)),
		)
	}

	settingsFd, err := os.Open(path.Join(tmpPath, "settings.json"))
	if err != nil {
		return err
	}
	defer settingsFd.Close()
	decoder := json.NewDecoder(settingsFd)
	if err := decoder.Decode(input.Settings()); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, input.path); err != nil {
		return err
	}

	input.Commit(size)

	return nil
}

// Input is a common.Input that can fetch the test case data from the grader.
type Input struct {
	runnerBaseInput
	requestURL string
	client     *http.Client
}

// Persist stores the Input into the filesystem.
func (input *Input) Persist() error {
	resp, err := input.client.Get(input.requestURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	uncompressedSize, err := strconv.ParseInt(
		resp.Header.Get("X-Content-Uncompressed-Size"), 10, 64,
	)
	if err != nil {
		return err
	}

	return input.persistFromTarStream(
		resp.Body,
		"gzip",
		uncompressedSize,
		resp.Header.Get("Content-SHA1"),
	)
}

// CachedInputFactory restores Inputs from a directory in the filesystem.
type CachedInputFactory struct {
	cachePath string
}

// NewCachedInputFactory creates a new CachedInputFactory.
func NewCachedInputFactory(cachePath string) common.CachedInputFactory {
	return &CachedInputFactory{
		cachePath: cachePath,
	}
}

// NewInput returns the Input for the provided hash.
func (factory *CachedInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &runnerBaseInput{
		BaseInput: *common.NewBaseInput(
			hash,
			mgr,
		),
		path: path.Join(factory.cachePath, fmt.Sprintf("%s/%s", hash[:2], hash[2:])),
	}
}

// GetInputHash returns the hash of the input located at the specified directory.
func (factory *CachedInputFactory) GetInputHash(
	dirname string,
	info os.FileInfo,
) (hash string, ok bool) {
	return fmt.Sprintf("%s%s", path.Base(dirname), info.Name()), info.IsDir()
}

type lazyReadCloser interface {
	Open() (io.ReadCloser, error)
}

// runnerTarInputFactory creates Inputs from a compressed .tar file.
type runnerTarInputFactory struct {
	inputPath string
	hash      string
	inputData lazyReadCloser
}

type runnerTarInput struct {
	runnerBaseInput
	factory *runnerTarInputFactory
}

func newRunnerTarInputFactory(
	config *common.Config,
	hash string,
	inputData lazyReadCloser,
) common.InputFactory {
	return &runnerTarInputFactory{
		inputPath: path.Join(
			config.Runner.RuntimePath,
			"input",
		),
		hash:      hash,
		inputData: inputData,
	}
}

func (factory *runnerTarInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &runnerTarInput{
		runnerBaseInput: runnerBaseInput{
			BaseInput: *common.NewBaseInput(
				hash,
				mgr,
			),
			path: path.Join(
				factory.inputPath,
				fmt.Sprintf("%s/%s", hash[:2], hash[2:]),
			),
		},
		factory: factory,
	}
}

func (factory *runnerTarInputFactory) GetInputHash(
	dirname string,
	info os.FileInfo,
) (hash string, ok bool) {
	return factory.hash, info.IsDir()
}

func (input *runnerTarInput) Persist() error {
	f, err := input.factory.inputData.Open()
	if err != nil {
		return err
	}
	hasher := common.NewHashReader(f, sha1.New())
	streamHash := fmt.Sprintf("%0x", hasher.Sum(nil))
	uncompressedSize := hasher.Length()
	f.Close()

	f, err = input.factory.inputData.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	return input.persistFromTarStream(
		f,
		"",
		uncompressedSize,
		streamHash,
	)
}
