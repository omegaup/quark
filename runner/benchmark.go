package runner

import (
	"bytes"
	"compress/bzip2"
	"encoding/base64"
	"github.com/omegaup/quark/common"
	"io"
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
		benchmarkCase{
			name: "CPU",
			hash: "1bb3ad5823ee268ba44496e07b3b497239781d5a",
			casefile: `QlpoOTFBWSZTWZnoAJIAByj/hP/+BARQZ/+QCgevm3//3/oACAAIUAV+qWNcACkGBcyEkIBQp6mm
QyYZTEDT1NNAAyAAaekGaKIpNpNAAAA0AAAAAAASeqlJNI0ABoAAAAGmgAAADhppkYjCaYCGATTC
MExMhpkaGgEUkk8kbU0AA0GgAAAGgGjTEPUwiVHveYdRtNtgJxJ8qAAgYSugqVtfa2G4ZcWG3ktA
kLQgSnkTAiqVVd8cUnIwbaopKegTHRKQCFRpQClBJkJKEgSTJCcRIHScRAnE6TicTogTjpkyY127
e/uwa+JmD0sbHWJKBXU1InNTTMp8eZ8pmzpGcSP2JCgGgCkBvrWWFIYSUMF/xueUpS9zfSBNZS/Z
s2uq9kJ1wCOgVVBW91tppve3tHwgCEHIQcNZUSVCwPAIYh2R0xQg4PEWMIquHixKysF2M57Gi6F0
mZhpVIZ3GYkQqUGxJoaOCkTZqX6itrcNbckVyYuxNyQjkItyRpCGlSEE1AGk9+EIicudOclujmYE
mY1GxUbFHOc3FcTLFmjFGNsMmJgJimjNJSWjERGiJKNRsiUW4LkMksaRS0xJAJAkkkTUao2xGMyQ
YJskUJtk2CoIphGMSmgwWLA1Vo0VFsaZWNGooiijaQsQaCnOVxTs85LSRJBIFAhOq51dLkOnOA5y
4q7C4IzqZyuYignXy5Iy6c4dicVFBJRbJBggcEs6Yuya1mpIbJbqQWW1ajFiBI1gsFoCGJAXXy52
W5myBuOZEuvv+ldGoAoIWgtLVYopDXXOEs5UkiQgZvpACjmbSzU2q2k0o05iNNZVjSpIpUKCaotq
0rRCBt2A84l1lLIzfEBrCHAGgYNAYIQzpjONMrGhgSIkCmSDDbFMrSVU0EqBtCo8Zl0xrjUSzpHG
IareO1mnKjunGq27xtJt46FwCZ9zVy7VmbRyQ0YFp06eCIjTk5mdmZnQaEtWrDAhgRB4HcTJCOTV
7OdfBcvR8doSW+sJIcRzJOCM2fNR5yzTottZy0CuI0uoKPZkounBF2xHCmsrVgYd6UTKE7CGUo2Q
hKQSvCyRVp4AnKpOars2UqppuCJVkSh3FdpXrFGhKmVKJEmVmCaFWFMETo+immDXPXH/0rawfjvS
VxD51qd3Ujecjaq7Cj2LRK76LZEt3Aun0CWCwQQecwJeMAHBDZGXxg/pgz4kCZ1ceLJBJwTAMGmN
8Lw7rlh2tgUdNlbeEfkRoR/GA64idXTsjGyxkyLKGWVxFx7vZwjPSiK9NNaFl3BMDowIxu3JdVLa
xLK1VFcEmq3KAS0aLdevDJmrQRCo/4u5IpwoSEz0AEkA`,
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
		benchmarkCase{
			name: "Memory",
			hash: "6d7367d7711c3c233dd9384e2d5b6ba2b05cc385",
			casefile: `QlpoOTFBWSZTWeNaK5AAAXF/hd+QBABQB//wCgevm3//3/oACEJABEhAAknWbBLBtRJTxE9TRoye
QmhoGgAHqDQABpvVBIiCaJTEeoDR6gAeoADIAaD1ADjJk00wmRkDAjE0YIwg0aYABBFQkj9UxNGQ
0Gg0AGgAyAND1HqA+EKOT0GNMqLgSoUqLSGSadABVfpmae9fdroqoRdKitHDaMpiDnKqtd1gRmhG
ABpCYKEmJiYmt2sW7hbDxsy3yikVlVUK1VU21r041xK4o4xIKAXAUgGytCwEtt8kpIYUaZyen1eu
IjjS6rYiofd8rLrrVDjtNtwpbNBQTNALZI1tVmohno2o/uVv7jjBo0Ipppuu8KdDlZ2ZmcSuBgsb
aQsToRIsSd0MAjex19xkMfh4zDfFcoEgJZWIJZwVqJRlHMIhajR0OSMxOOF01VZThqWGYMEOFHL0
C6vazTunM3M5V2I0FfWFZSoXZmS2OFS0a1QCszjIrnUzeae3LFVe8p3gEXTiS2cZ2eV5C2SWJ3kr
ZWvi07LTLxdXTEaEKgIgYYVFXPSj7aSXZ6LP9KRV7tsRKga2TIp4Ae1RCRJ6C/yNhHJ8Et4Rt9we
p7glgsECB4SBKSoDBBtDHHV+yC7CIJoTHkzQTIBKhgXh+QpHgbgXbYy+5s9USkR1kflgcAit4UNG
etmmi1BqpgRgjEZQE79pRTZZcwii8hknFZQrOCwpIyz5kK0EYqdIUI7syQtZVESv8chIyZgR/i7k
inChIca0VyA=
`,
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
	Memory   common.Byte
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
		input, err := inputManager.Add(
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
		defer input.Release(input)

		run := common.Run{
			AttemptID: uint64(idx),
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

	return benchmarkResults, nil
}
