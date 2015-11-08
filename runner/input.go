package runner

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/omegaup/quark/common"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type baseRunnerInput struct {
	common.BaseInput
	path string
}

type RunnerInput struct {
	baseRunnerInput
	requestURL string
	client     *http.Client
}

type RunnerInputFactory struct {
	run    *common.Run
	client *http.Client
	config *common.Config
}

type RunnerCachedInputFactory struct {
	config *common.Config
	hash   string
}

func NewRunnerCachedInputFactory(hash string, config *common.Config) common.InputFactory {
	return &RunnerCachedInputFactory{
		config: config,
		hash:   hash,
	}
}

func (factory *RunnerCachedInputFactory) NewInput(mgr *common.InputManager) common.Input {
	return &baseRunnerInput{
		BaseInput: *common.NewBaseInput(factory.hash, mgr),
		path: path.Join(factory.config.Runner.RuntimePath,
			"input", factory.hash),
	}
}

func PreloadInputs(ctx *common.Context, ioLock *sync.Mutex) error {
	path := path.Join(ctx.Config.Runner.RuntimePath, "input")
	contents, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, info := range contents {
		if !info.IsDir() {
			continue
		}
		hash := info.Name()
		factory := NewRunnerCachedInputFactory(hash, &ctx.Config)

		// Make sure no other I/O is being made while we pre-fetch this input.
		ioLock.Lock()
		input, err := common.DefaultInputManager.Add(info.Name(), factory)
		if err != nil {
			ctx.Log.Error("Cached input corrupted", "hash", hash)
		} else {
			input.Release()
		}
		ioLock.Unlock()
	}
	ctx.Log.Info("Finished preloading cached inputs",
		"cache_size", common.DefaultInputManager.Size())
	return nil
}

func NewRunnerInputFactory(run *common.Run, client *http.Client, config *common.Config) common.InputFactory {
	return &RunnerInputFactory{
		run:    run,
		client: client,
		config: config,
	}
}

func (factory *RunnerInputFactory) NewInput(mgr *common.InputManager) common.Input {
	baseURL, err := url.Parse(factory.config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	requestURL, err := baseURL.Parse("input/" + factory.run.InputHash)
	if err != nil {
		panic(err)
	}
	return &RunnerInput{
		baseRunnerInput: baseRunnerInput{
			BaseInput: *common.NewBaseInput(factory.run.InputHash, mgr),
			path: path.Join(factory.config.Runner.RuntimePath,
				"input", factory.run.InputHash),
		},
		client:     factory.client,
		requestURL: requestURL.String(),
	}
}

func (input *baseRunnerInput) getStoredHashes() (map[string]string, error) {
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
	sha1sumRe := regexp.MustCompile("^([a-f0-9]{40}) [ *](.*)$")
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

func (input *baseRunnerInput) Verify() error {
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
			return errors.New(fmt.Sprintf("hash mismatch for '%s'", path))
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

func (input *baseRunnerInput) CreateArchive() error {
	return errors.New("baseRunnerInput cannot create archives")
}

func (input *RunnerInput) CreateArchive() error {
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
				return err
			}
		} else {
			if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
				panic(err)
				return err
			}
			fd, err := os.Create(filePath)
			if err != nil {
				panic(err)
				return err
			}
			defer fd.Close()

			innerHasher := common.NewHashReader(archive, sha1.New())
			if _, err := io.Copy(fd, innerHasher); err != nil {
				panic(err)
				return err
			}
			_, err = fmt.Fprintf(sha1sumFile, "%0x *%s/%s\n", innerHasher.Sum(nil),
				input.Hash(), hdr.Name)
			if err != nil {
				panic(err)
				return err
			}
			size += hdr.Size
		}
	}

	if resp.Header.Get("Content-SHA1") != fmt.Sprintf("%0x", hasher.Sum(nil)) {
		return errors.New(fmt.Sprintf("hash mismatch: expected %s got %s",
			resp.Header.Get("Content-SHA1"), fmt.Sprintf("%0x", hasher.Sum(nil))))
	}

	if err := os.Rename(tmpPath, input.path); err != nil {
		return err
	}

	input.Commit(size)
	return nil
}

func (input *RunnerInput) DeleteArchive() error {
	os.RemoveAll(fmt.Sprintf("%s.tmp", input.path))
	os.Remove(fmt.Sprintf("%s.sha1", input.path))
	return os.RemoveAll(input.path)
}
