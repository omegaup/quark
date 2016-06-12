package runner

import (
	"bytes"
	"compress/bzip2"
	"encoding/base64"
	"encoding/json"
	"github.com/lhchavez/quark/common"
	"io"
	"net/http"
	"net/url"
	"sync"
)

type benchmarkCase struct {
	name     string
	casefile string
	hash     string
	source   string
	language string
}

type benchmarkFile struct {
	io.Reader
	buffer *bytes.Buffer
}

func (b *benchmarkCase) Open() (io.ReadCloser, error) {
	buf := bytes.NewBufferString(b.casefile)
	return &benchmarkFile{
		Reader: bzip2.NewReader(base64.NewDecoder(base64.StdEncoding, buf)),
		buffer: buf,
	}, nil
}

func (b *benchmarkFile) Close() error {
	return nil
}

var (
	cases = []benchmarkCase{
		benchmarkCase{
			name: "IO",
			hash: "bfd75d2b21fcf16862aa0ebf356ed1dcc380982e",
			casefile: `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`,
			source: `#include <iostream>
int main() {
				std::cin.tie(0);
				std::ios_base::sync_with_stdio(0);

				int N, A, x, pos = 0;
				std::cin >> N >> A;
				while (--N) {
								std::cin >> x;
								if (A > x) ++pos;
				}
				std::cout << pos << std::endl;
}`,
			language: "cpp11",
		},
	}
)

type BenchmarkResult struct {
	Time     float64
	WallTime float64
	Memory   int64
}

type BenchmarkResults map[string]BenchmarkResult

func RunHostBenchmark(
	ctx *common.Context,
	client *http.Client,
	baseURL *url.URL,
	inputManager *common.InputManager,
	sandbox Sandbox,
	ioLock sync.Locker,
) (BenchmarkResults, error) {
	ioLock.Lock()
	defer ioLock.Unlock()

	ctx.Log.Info("Running benchmark")

	benchmarkResults := make(BenchmarkResults)
	for _, benchmarkCase := range cases {
		input, err := inputManager.Add(
			benchmarkCase.hash,
			NewRunnerTarInputFactory(
				&ctx.Config,
				benchmarkCase.hash,
				&benchmarkCase,
			),
		)
		if err != nil {
			return nil, err
		}
		defer input.Release(input)

		run := common.Run{
			Source:    benchmarkCase.source,
			Language:  benchmarkCase.language,
			InputHash: benchmarkCase.hash,
			MaxScore:  1.0,
			Debug:     false,
		}
		results, err := Grade(ctx, nil, &run, input, sandbox)
		if err != nil {
			return nil, err
		}

		benchmarkResults[benchmarkCase.name] = BenchmarkResult{
			Time:     results.Time,
			WallTime: results.WallTime,
			Memory:   results.Memory,
		}
	}

	buf, err := json.Marshal(&benchmarkResults)
	if err != nil {
		return nil, err
	}
	benchmarkURL, err := url.Parse("monitoring/benchmark/")
	if err != nil {
		panic(err)
	}
	resp, err := client.Post(
		baseURL.ResolveReference(benchmarkURL).String(),
		"text/json",
		bytes.NewReader(buf),
	)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return benchmarkResults, nil
}
