package runner

import (
	"archive/tar"
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
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

// RunnerInputFactory is an InputFactory that can fetch the test case data from
// the grader.
type RunnerInputFactory struct {
	client *http.Client
	config *common.Config
}

func NewRunnerInputFactory(
	client *http.Client,
	config *common.Config,
) common.InputFactory {
	return &RunnerInputFactory{
		client: client,
		config: config,
	}
}

func (factory *RunnerInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	baseURL, err := url.Parse(factory.config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	requestURL, err := baseURL.Parse(fmt.Sprintf("input/%s/", hash))
	if err != nil {
		panic(err)
	}
	return &RunnerInput{
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

	var size int64 = 0
	for path, expectedHashStr := range hashes {
		actualHash, err := common.Sha1sum(path)
		if err != nil {
			return err
		}
		actualHashStr := fmt.Sprintf("%0x", actualHash)
		if actualHashStr != expectedHashStr {
			return errors.New(fmt.Sprintf(
				"hash mismatch for '%s' == %q, want %q",
				path,
				actualHashStr,
				expectedHashStr,
			))
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
			return result, errors.New(fmt.Sprintf("sha1sum file format error '%s",
				hashPath))
		}
		expectedHash := res[1]
		filePath := filepath.Join(dir, res[2])
		if !strings.HasPrefix(filePath, input.path) {
			return result, errors.New(fmt.Sprintf("path is outside expected directory: '%s",
				filePath))
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

	var size int64 = 0

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
		} else {
			if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
				return err
			}
			fd, err := os.Create(filePath)
			if err != nil {
				return err
			}
			defer fd.Close()

			innerHasher := common.NewHashReader(archive, sha1.New())
			if _, err := io.Copy(fd, innerHasher); err != nil {
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
	}

	if streamHash != fmt.Sprintf("%0x", hasher.Sum(nil)) {
		return errors.New(fmt.Sprintf(
			"hash mismatch: expected %s got %s",
			streamHash,
			fmt.Sprintf("%0x", hasher.Sum(nil)),
		))
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

// RunnerInput is an Input that can fetch the test case data from the grader.
type RunnerInput struct {
	runnerBaseInput
	requestURL string
	client     *http.Client
}

func (input *RunnerInput) Persist() error {
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

// RunnerCachedInputFactory restores Inputs from a directory in the filesystem.
type RunnerCachedInputFactory struct {
	cachePath string
}

func NewRunnerCachedInputFactory(cachePath string) common.CachedInputFactory {
	return &RunnerCachedInputFactory{
		cachePath: cachePath,
	}
}

func (factory *RunnerCachedInputFactory) NewInput(
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

func (factory *RunnerCachedInputFactory) GetInputHash(
	dirname string,
	info os.FileInfo,
) (hash string, ok bool) {
	return fmt.Sprintf("%s%s", path.Base(dirname), info.Name()), info.IsDir()
}

type LazyReadCloser interface {
	Open() (io.ReadCloser, error)
}

// runnerTarInputFactory creates Inputs from a compressed .tar file.
type runnerTarInputFactory struct {
	inputPath string
	hash      string
	inputData LazyReadCloser
}

type runnerTarInput struct {
	runnerBaseInput
	factory *runnerTarInputFactory
}

func NewRunnerTarInputFactory(
	config *common.Config,
	hash string,
	inputData LazyReadCloser,
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
