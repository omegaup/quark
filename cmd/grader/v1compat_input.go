package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	git "github.com/libgit2/git2go"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type v1CompatGraderBaseInput struct {
	common.BaseInput
	archivePath      string
	storedHash       string
	uncompressedSize int64
}

func (input *v1CompatGraderBaseInput) Verify() error {
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
		return errors.New("Hash verification failed")
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

func (input *v1CompatGraderBaseInput) getStoredHash() (string, error) {
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

func (input *v1CompatGraderBaseInput) getStoredLength() (int64, error) {
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

func (input *v1CompatGraderBaseInput) Delete() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
	os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
	os.Remove(fmt.Sprintf("%s.len", input.archivePath))
	return os.Remove(input.archivePath)
}

// Transmit sends a serialized version of the Input to the runner. It sends a
// .tar.gz file with the Content-SHA1 header with the hexadecimal
// representation of its SHA-1 hash.
func (input *v1CompatGraderBaseInput) Transmit(w http.ResponseWriter) error {
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

// v1CompatGraderInput is an Input generated from a git repository that is then stored
// in a .tar.gz file that can be sent to a runner.
type v1CompatGraderInput struct {
	v1CompatGraderBaseInput
	repositoryPath string
	db             *sql.DB
	problemName    string
}

func (input *v1CompatGraderInput) Persist() error {
	if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.archivePath)
	defer os.Remove(tmpPath)
	uncompressedSize, err := input.createArchiveFromGit(tmpPath)
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

func (input *v1CompatGraderInput) createArchiveFromGit(
	archivePath string,
) (int64, error) {
	err := input.db.QueryRow(
		`SELECT
			extra_wall_time, memory_limit, output_limit, overall_wall_time_limit,
			time_limit, validator_time_limit, slow, validator
		FROM
			Problems
		WHERE
			alias = ?;`, input.problemName).Scan(
		&input.Settings().Limits.ExtraWallTime,
		&input.Settings().Limits.MemoryLimit,
		&input.Settings().Limits.OutputLimit,
		&input.Settings().Limits.OverallWallTimeLimit,
		&input.Settings().Limits.TimeLimit,
		&input.Settings().Limits.ValidatorTimeLimit,
		&input.Settings().Slow,
		&input.Settings().Validator.Name,
	)
	if err != nil {
		return 0, err
	}
	input.Settings().Limits.MemoryLimit *= 1024
	if input.Settings().Validator.Name == "token-numeric" {
		tolerance := 1e-6
		input.Settings().Validator.Tolerance = &tolerance
	}

	repository, err := git.OpenRepository(input.repositoryPath)
	if err != nil {
		return 0, err
	}
	defer repository.Free()

	treeOid, err := git.NewOid(input.Hash())
	if err != nil {
		return 0, err
	}

	tree, err := repository.LookupTree(treeOid)
	if err != nil {
		return 0, err
	}
	defer tree.Free()
	odb, err := repository.Odb()
	if err != nil {
		return 0, err
	}
	defer odb.Free()

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		return 0, err
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	// TODO(lhchavez): Support libinteractive.
	var walkErr error = nil
	var uncompressedSize int64 = 0
	rawCaseWeights := make(map[string]float64)
	tree.Walk(func(parent string, entry *git.TreeEntry) int {
		untrimmedPath := path.Join(parent, entry.Name)
		if untrimmedPath == "testplan" && entry.Type == git.ObjectBlob {
			blob, walkErr := repository.LookupBlob(entry.Id)
			if walkErr != nil {
				return -1
			}
			defer blob.Free()
			testplanRe := regexp.MustCompile(`^\s*([^# \t]+)\s+([0-9.]+).*$`)
			for _, line := range strings.Split(string(blob.Contents()), "\n") {
				m := testplanRe.FindStringSubmatch(line)
				if m == nil {
					continue
				}
				rawCaseWeights[m[1]], walkErr = strconv.ParseFloat(m[2], 64)
				if walkErr != nil {
					return -1
				}
			}
		}
		if strings.HasPrefix(untrimmedPath, "validator.") &&
			input.Settings().Validator.Name == "custom" &&
			entry.Type == git.ObjectBlob {
			lang := filepath.Ext(untrimmedPath)
			input.Settings().Validator.Lang = &lang
			blob, walkErr := repository.LookupBlob(entry.Id)
			if walkErr != nil {
				return -1
			}
			defer blob.Free()
			hdr := &tar.Header{
				Name:     untrimmedPath,
				Typeflag: tar.TypeReg,
				Mode:     0644,
				Size:     blob.Size(),
			}
			uncompressedSize += blob.Size()
			if walkErr = archive.WriteHeader(hdr); walkErr != nil {
				return -1
			}
			if _, walkErr := archive.Write(blob.Contents()); walkErr != nil {
				return -1
			}
		}
		if !strings.HasPrefix(untrimmedPath, "cases/") {
			return 0
		}
		entryPath := strings.TrimPrefix(untrimmedPath, "cases/")
		if strings.HasPrefix(entryPath, "in/") {
			caseName := strings.TrimSuffix(strings.TrimPrefix(entryPath, "in/"), ".in")
			if _, ok := rawCaseWeights[caseName]; !ok {
				rawCaseWeights[caseName] = 1.0
			}
		}
		switch entry.Type {
		case git.ObjectTree:
			hdr := &tar.Header{
				Name:     entryPath,
				Typeflag: tar.TypeDir,
				Mode:     0755,
				Size:     0,
			}
			if walkErr = archive.WriteHeader(hdr); walkErr != nil {
				return -1
			}
		case git.ObjectBlob:
			blob, walkErr := repository.LookupBlob(entry.Id)
			if walkErr != nil {
				return -1
			}
			defer blob.Free()

			hdr := &tar.Header{
				Name:     entryPath,
				Typeflag: tar.TypeReg,
				Mode:     0644,
				Size:     blob.Size(),
			}
			uncompressedSize += blob.Size()
			if walkErr = archive.WriteHeader(hdr); walkErr != nil {
				return -1
			}

			stream, err := odb.NewReadStream(entry.Id)
			if err == nil {
				defer stream.Free()
				if _, walkErr := io.Copy(archive, stream); walkErr != nil {
					return -1
				}
			} else {
				// That particular object cannot be streamed. Allocate the blob in
				// memory and write it to the archive.
				if _, walkErr := archive.Write(blob.Contents()); walkErr != nil {
					return -1
				}
			}
		}
		return 0
	})
	if walkErr != nil {
		return 0, walkErr
	}

	// Generate the group/case settings.
	cases := make(map[string][]common.CaseSettings)
	groupWeights := make(map[string]float64)
	totalWeight := 0.0
	for caseName, weight := range rawCaseWeights {
		components := strings.SplitN(caseName, ".", 2)
		groupName := components[0]
		if _, ok := groupWeights[groupName]; !ok {
			groupWeights[groupName] = 0
		}
		groupWeights[groupName] += weight
		totalWeight += weight
		if _, ok := cases[groupName]; !ok {
			cases[groupName] = make([]common.CaseSettings, 0)
		}
		cases[groupName] = append(cases[groupName], common.CaseSettings{
			Name:   caseName,
			Weight: weight,
		})
	}
	input.Settings().Cases = make([]common.GroupSettings, 0)
	for groupName, cases := range cases {
		sort.Sort(common.ByCaseName(cases))
		input.Settings().Cases = append(input.Settings().Cases, common.GroupSettings{
			Cases:  cases,
			Name:   groupName,
			Weight: groupWeights[groupName] / totalWeight,
		})
	}
	sort.Sort(common.ByGroupName(input.Settings().Cases))

	// Finally, write settings.json.
	settingsBlob, err := json.MarshalIndent(input.Settings(), "", "  ")
	if err != nil {
		return 0, err
	}
	hdr := &tar.Header{
		Name:     "settings.json",
		Typeflag: tar.TypeReg,
		Mode:     0644,
		Size:     int64(len(settingsBlob)),
	}
	uncompressedSize += int64(len(settingsBlob))
	if err = archive.WriteHeader(hdr); err != nil {
		return 0, err
	}
	if _, err = archive.Write(settingsBlob); err != nil {
		return 0, err
	}

	return uncompressedSize, nil
}

// v1CompatGraderInputFactory is an InputFactory that can store specific versions of a
// problem's git repository into a .tar.gz file that can be easily shipped to
// runners.
type v1CompatGraderInputFactory struct {
	problemName string
	config      *common.Config
	db          *sql.DB
}

func v1CompatNewGraderInputFactory(
	problemName string,
	config *common.Config,
	db *sql.DB,
) common.InputFactory {
	return &v1CompatGraderInputFactory{
		problemName: problemName,
		config:      config,
		db:          db,
	}
}

func (factory *v1CompatGraderInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &v1CompatGraderInput{
		v1CompatGraderBaseInput: v1CompatGraderBaseInput{
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
		repositoryPath: path.Join(
			factory.config.Grader.V1.RuntimePath,
			"problems.git",
			factory.problemName,
		),
		db:          factory.db,
		problemName: factory.problemName,
	}
}

// v1CompatGraderCachedInputFactory is a grader-specific CachedInputFactory.
type v1CompatGraderCachedInputFactory struct {
	inputPath string
}

func NewGraderCachedInputFactory(inputPath string) common.CachedInputFactory {
	return &v1CompatGraderCachedInputFactory{
		inputPath: inputPath,
	}
}

func (factory *v1CompatGraderCachedInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &v1CompatGraderBaseInput{
		BaseInput: *common.NewBaseInput(
			hash,
			mgr,
		),
		archivePath: path.Join(
			factory.inputPath,
			fmt.Sprintf("%s/%s.tar.gz", hash[:2], hash[2:]),
		),
	}
}

func (factory *v1CompatGraderCachedInputFactory) GetInputHash(
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
