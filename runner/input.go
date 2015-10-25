package runner

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/omegaup/quark/context"
	"github.com/omegaup/quark/queue"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

type baseRunnerInput struct {
	context.BaseInput
	path string
}

type RunnerInput struct {
	baseRunnerInput
	requestURL string
	client     *http.Client
}

type RunnerInputFactory struct {
	run    *queue.Run
	client *http.Client
	config *context.Config
}

func NewRunnerInputFactory(run *queue.Run, client *http.Client, config *context.Config) context.InputFactory {
	return &RunnerInputFactory{
		run:    run,
		client: client,
		config: config,
	}
}

func (factory *RunnerInputFactory) NewInput(mgr *context.InputManager) context.Input {
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
			BaseInput: *context.NewBaseInput(factory.run.InputHash, mgr),
			path: path.Join(factory.config.Runner.RuntimePath,
				"input", factory.run.InputHash),
		},
		client:     factory.client,
		requestURL: requestURL.String(),
	}
}

func (input *baseRunnerInput) Verify() error {
	_, err := os.Stat(input.path)
	if err != nil {
		return err
	}

	var size int64 = 0
	err = filepath.Walk(input.path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		return err
	}

	input.Commit(size)
	return nil
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

	hasher := context.NewHashReader(resp.Body, sha1.New())

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

			innerHasher := context.NewHashReader(archive, sha1.New())
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

func sha1sum(path string) ([]byte, error) {
	hash := sha1.New()
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	if _, err := io.Copy(hash, fd); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}
