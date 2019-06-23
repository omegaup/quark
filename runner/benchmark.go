package runner

import (
	"bytes"
	"compress/bzip2"
	"encoding/base64"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"io"
	"math/big"
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
		{
			name: "IO",
			hash: "cc1d0fbdf968f8dd33c5679a9ca01dde11f453e3",
			casefile: `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
WmVvrrK/4u5IpwoSAlZBXEA=`,
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
		{
			name: "CPU",
			hash: "1266cc8132d95cfe8e7a19e4492627d361c39308",
			casefile: `QlpoOTFBWSZTWQ1oDZQABx5/hP/+AgFQZ/+QCgety3//3+oABAAIUAVc82414AVSDdDyoSIiNEJo
hoG0xQaA2oAA0AaDT9UGaRCkagAAAGmgAAAAAASeqlSm0jTIAaaaAAAYCAAAAEUVQ9T0TQAA0zUA
0AAHqGjQAARU1FMmnlPUA00AAAAAAyMmjR6jAh/GD2YdUtuuQFCFGXAgQMJSCKXKqoiY2di2t0Ub
qC2rLSIULjlMyuE2GzolKFpQpVkAtKAUoJMhChAJDCMCThCwJMMDCwMDCTJBDJkyK7Wlq3JivbbD
0s4NJRSLBVVCtVVNqvc0RnNGkRpQhc6CFANAFIC8uGsKQQkwwfFfd3zM31qiBrCX7NuzrvbCdkAj
oClSCm+E2003vb2XxQCEHIQcNSkSUhWOwQxDqR0XQQcHcV2ilVu6iVSmC38Y38ro1mYll6VEgbKE
q64eNHLnZdNW7sMi+1u1fbwzfmxwZGeIcKQi4EjSENKiEE1AGk+CEIiRSpEt0OZgSZjUbFRsUc5z
cVxMsWaMUY2wyYmAmKaM0lJaMREaIko1GyJRbguQySxpFLTEkAkCSSRNRqjbEYzJBgmyRQm2TYKg
imEYxKaDBYsDVWjRUWxplY0aiiKKNpCxBoKc5XFOrzktJEkEgUCAi1LpRAVIAEigksCgCBljIlzE
UE6eXJGXRzh1JxUXFXLbIMEDgljN1tmqZokNsqtEFhtJpAyxAkawWC0BDEgLp5c6rczZA3HMiXT1
+iujXBQQsizVKoopDWsWljCkkSEDNyQAocxVFTRsptJpRpy401hVGlRIpSFBNULZVFNEIG7UB4uV
qUVIzcIDVocAaBg0BZCGM3i84V5LEiJApggw3RTCzKVGRKgNgqHeJWb1ehLGY4KjR3LWdaXRCgd7
4u66PUcy1icdjx7tnD4jFRiLbt265mZ8sDAFxaFCDbmRhUL3L2Vtb4X8pR02wiSgYg3YszoIZ5hB
RdNbxWUcbVkDssIEGBOxO5X76C4vScq8jgChQK1xlBZ+eiV146oKVbbrelosNBUQNkpXUhBGdKyW
KDCsy1IgoaxCbRqOXKJSKA2v3umGaogVNAbzKBJjuLN0vEX7y1IbXqElvmAQoKCRIuyXCYgOCSJU
MYMVcyluWhcELQ9FQ2CceMMI8i4dIo6L1fxo5WBnCogYqRCh240HOg35M2ZorEJ6EKCKIx4CbKhb
NgG3XPaBqrGQRkIY9nApldaggyQby6MiMuTiOsvsvV/IU0Y2Al/xdyRThQkA1oDZQA==`,
			source: `#include <iostream>
#include <cstring>

constexpr size_t N = 2048;

bool DP[N][N];
char input[N];

int main() {
        std::cin.tie(0);
        std::ios_base::sync_with_stdio(0);

        std::cin >> input;

        size_t len = strlen(input);
        for (size_t i = 0; i < len; i++) {
                DP[0][i] = true;
        }
        for (size_t w = 2; w <= len; w += 2) {
                for (size_t i = 0; i + w <= len; i++) {
                        for (size_t j = 2; j < w; j += 2) {
                                DP[w][i] |= DP[j][i] && DP[w - j][i + j];
                        }
                        DP[w][i] |= (input[i] == '(') && (input[i + w - 1] == ')') &&
                                DP[w - 2][i + 1];
                }
        }
        std::cout << DP[len][0] << std::endl;
}`,
			language: "cpp11",
		},
		{
			name: "Memory",
			hash: "29add4b8d272e18b796237570454c30fb2fcb9bc",
			casefile: `QlpoOTFBWSZTWXS9po8AAWb/hd+QAgBQB//wCgety3//3+oABEJABBhAAi7VtgjBJIVPU9BPUaaa
fqEaAABiBppoABv1UEiQTIRQNMQ000DNI9TI00YmIDQA5gCMExAMAmCaMhoYBMEYmEUk1GkfqjQG
gGg0AAADQGmgaMqB8vmmWqXTWSoQUCqEwmpQJK7em1N8a9cSpZEK1sGpNCRNncJoY6hWrALAMlgT
ExM7O8Ls8LYa2XzJqQqqimUoqJtU8VoxQY6I+qEKAZoKQG0s1gTNdkqKiUBNpn05PfyxEfFLdbEU
D7vRbeWcOPO23Ck25EhtFK0pNaO1WxNCzvPy/jju8rIpzncjGMephMBbSs726t6oBmXiUhhQbG/2
9z3t/ysjdUbQhCDEGeeaCBxrEElVLRG/7lYwTWtrFBsqL9yBBCs8QorJY5qClkFqZXi7CFXuhSc2
poIgFK8XhK2Be1EF2tU0tMykpEUyEDZeRWdUGGhLLJBXAljuTtWuSZgILDUKl60fmSS3uBn+kkUB
tfnVKGbsQKekNScCJHIYdR1kfXrS0obd8IX3MAhQoQQecLhPsDhhEtjLB94qlNCLmmIYw9dsbJMe
MMI77cyhl+1td1OaAMgLAhAzlJlFq6hhQy6ilGmIQnRChKkrUgq04JaNALXzyrAqs4mCFdBXv1iT
0UyEDoKl+uQfh2Cadd33BRkwzAl/i7kinChIOl7TR4A=`,
			source: `#include <iostream>
#include <cstring>
#include <memory>

int main() {
        std::cin.tie(0);
        std::ios_base::sync_with_stdio(0);

        int N, x, y;
        std::cin >> N >> x >> y;
                                std::unique_ptr<int[]> arr(new int[N]);
                                for (int i = 0, j = 0; i < y; i++) {
                                        arr[j]++;
                                        j += x;
                                        if (j >= N) j = 0;
                                }
                                int sum = 0;
                                for (int i = 0; i < N; i++) {
                                        sum += arr[i];
                                }
        std::cout << sum << std::endl;
}`,
			language: "cpp11",
		},
	}
)

// A BenchmarkResult represents the result of a single benchmark run.
type BenchmarkResult struct {
	Time     float64
	WallTime float64
	Memory   base.Byte
}

// BenchmarkResults represents the results of running the whole suite of
// benchmarks.
type BenchmarkResults map[string]BenchmarkResult

// RunHostBenchmark runs the benchmarking suite and returns its results.
func RunHostBenchmark(
	ctx *common.Context,
	inputManager *common.InputManager,
	sandbox Sandbox,
	ioLock sync.Locker,
) (BenchmarkResults, error) {
	ioLock.Lock()
	defer ioLock.Unlock()

	ctx.Log.Info("Running benchmark")

	benchmarkResults := make(BenchmarkResults)
	for idx, benchmarkCase := range cases {
		inputRef, err := inputManager.Add(
			benchmarkCase.hash,
			newRunnerTarInputFactory(
				&ctx.Config,
				benchmarkCase.hash,
				&benchmarkCase,
			),
		)
		if err != nil {
			return nil, err
		}
		defer inputRef.Release()

		run := common.Run{
			AttemptID: uint64(idx),
			Source:    benchmarkCase.source,
			Language:  benchmarkCase.language,
			InputHash: benchmarkCase.hash,
			MaxScore:  big.NewRat(1, 1),
			Debug:     false,
		}
		results, err := Grade(ctx, nil, &run, inputRef.Input, sandbox)
		if err != nil {
			return nil, err
		}

		benchmarkResults[benchmarkCase.name] = BenchmarkResult{
			Time:     results.Time,
			WallTime: results.WallTime,
			Memory:   results.Memory,
		}
	}

	return benchmarkResults, nil
}
