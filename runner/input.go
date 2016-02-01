package runner

import (
	"archive/tar"
	"bufio"
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
	os.Remove(fmt.Sprintf("%s.sha1", input.path))
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
	sha1sumRe := regexp.MustCompile("^\\s*([a-f0-9]{40}) [ *](\\S+)\\s*$")
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

// RunnerInput is an Input that can fetch the test case data from the grader.
type RunnerInput struct {
	runnerBaseInput
	requestURL string
	client     *http.Client
}

func (input *RunnerInput) Persist() error {
	tmpPath := fmt.Sprintf("%s.tmp", input.path)
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpPath)

	resp, err := input.client.Get(input.requestURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	hasher := common.NewHashReader(resp.Body, sha1.New())

	gz, err := gzip.NewReader(hasher)
	if err != nil {
		return err
	}
	defer gz.Close()

	archive := tar.NewReader(gz)

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
				panic(err)
			}
		} else {
			if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
				panic(err)
			}
			fd, err := os.Create(filePath)
			if err != nil {
				panic(err)
			}
			defer fd.Close()

			innerHasher := common.NewHashReader(archive, sha1.New())
			if _, err := io.Copy(fd, innerHasher); err != nil {
				panic(err)
			}
			_, err = fmt.Fprintf(
				sha1sumFile,
				"%0x *%s/%s\n",
				innerHasher.Sum(nil),
				input.Hash(),
				hdr.Name,
			)
			if err != nil {
				panic(err)
			}
			size += hdr.Size
		}
	}

	if resp.Header.Get("Content-SHA1") != fmt.Sprintf("%0x", hasher.Sum(nil)) {
		return errors.New(fmt.Sprintf(
			"hash mismatch: expected %s got %s",
			resp.Header.Get("Content-SHA1"),
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
		path: path.Join(factory.cachePath, hash),
	}
}

func (factory *RunnerCachedInputFactory) GetInputHash(
	info os.FileInfo,
) (hash string, ok bool) {
	return info.Name(), info.IsDir()
}
