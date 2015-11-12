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

type RunnerInput struct {
	common.BaseInput
	requestURL string
	client     *http.Client
}

type RunnerInputFactory struct {
	run    *common.Run
	client *http.Client
	config *common.Config
}

type RunnerCachedInputFactory struct {
	cachePath string
}

func NewRunnerCachedInputFactory(cachePath string) common.InputFactory {
	return &RunnerCachedInputFactory{
		cachePath: cachePath,
	}
}

func (factory *RunnerCachedInputFactory) NewInput(hash string, mgr *common.InputManager) common.Input {
	return &RunnerInput{
		BaseInput: *common.NewBaseInput(
			hash,
			mgr,
			path.Join(factory.cachePath, hash)),
	}
}

func RunnerCachedInputFilter(info os.FileInfo) (string, bool) {
	return info.Name(), info.IsDir()
}

func NewRunnerInputFactory(run *common.Run, client *http.Client, config *common.Config) common.InputFactory {
	return &RunnerInputFactory{
		run:    run,
		client: client,
		config: config,
	}
}

func (factory *RunnerInputFactory) NewInput(hash string, mgr *common.InputManager) common.Input {
	baseURL, err := url.Parse(factory.config.Runner.GraderURL)
	if err != nil {
		panic(err)
	}
	requestURL, err := baseURL.Parse("input/" + factory.run.InputHash)
	if err != nil {
		panic(err)
	}
	return &RunnerInput{
		BaseInput: *common.NewBaseInput(factory.run.InputHash, mgr,
			path.Join(factory.config.Runner.RuntimePath,
				"input", factory.run.InputHash)),
		client:     factory.client,
		requestURL: requestURL.String(),
	}
}

func (input *RunnerInput) getStoredHashes() (map[string]string, error) {
	result := make(map[string]string)
	dir := filepath.Dir(input.Path())
	hashPath := fmt.Sprintf("%s.sha1", input.Path())
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
		if !strings.HasPrefix(filePath, input.Path()) {
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

func (input *RunnerInput) Verify() error {
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

	settingsFd, err := os.Open(path.Join(input.Path(), "settings.json"))
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

func (input *RunnerInput) CreateArchive() error {
	tmpPath := fmt.Sprintf("%s.tmp", input.Path())
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

	sha1sumFile, err := os.Create(fmt.Sprintf("%s.sha1", input.Path()))
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

	if err := os.Rename(tmpPath, input.Path()); err != nil {
		return err
	}

	settingsFd, err := os.Open(path.Join(input.Path(), "settings.json"))
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

func (input *RunnerInput) DeleteArchive() error {
	os.RemoveAll(fmt.Sprintf("%s.tmp", input.Path()))
	os.Remove(fmt.Sprintf("%s.sha1", input.Path()))
	return os.RemoveAll(input.Path())
}
