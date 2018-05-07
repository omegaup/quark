package v1compat

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	git "github.com/libgit2/git2go"
	"github.com/omegaup/quark/common"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	// InputVersion represents a global version. Bump if a fundamentally breaking
	// change is introduced.
	InputVersion = 1
)

// A SettingsLoader allows to load ProblemSettings from a particular git tree
// hash.
type SettingsLoader struct {
	Settings *common.ProblemSettings
	GitTree  string
}

type graderBaseInput struct {
	common.BaseInput
	archivePath      string
	storedHash       string
	uncompressedSize int64
}

// A ProblemInformation represents information from the problem.
type ProblemInformation struct {
	TreeID        string
	IsInteractive bool
}

// VersionedHash returns the hash for a specified problem. It takes into
// account the global InputVersion, the libinteractive version (for interactive
// problems), and the hash of the tree in git.
func VersionedHash(
	libinteractiveVersion string,
	problemInfo *ProblemInformation,
	settings *common.ProblemSettings,
) string {
	hasher := sha1.New()
	fmt.Fprintf(
		hasher,
		"%d:%s:",
		InputVersion,
		problemInfo.TreeID,
	)
	if problemInfo.IsInteractive {
		fmt.Fprintf(
			hasher,
			"%s:",
			libinteractiveVersion,
		)
	}
	json.NewEncoder(hasher).Encode(settings)
	return fmt.Sprintf("%0x", hasher.Sum(nil))
}

// GetProblemInformation returns the ProblemInformation obtained from the git
// repository located at the provided repository path.
func GetProblemInformation(repositoryPath string) (*ProblemInformation, error) {
	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return nil, err
	}
	defer repository.Free()
	headRef, err := repository.Head()
	if err != nil {
		return nil, err
	}
	defer headRef.Free()
	headObject, err := headRef.Peel(git.ObjectCommit)
	if err != nil {
		return nil, err
	}
	defer headObject.Free()
	headCommit, err := headObject.AsCommit()
	if err != nil {
		return nil, err
	}
	defer headCommit.Free()
	headTree, err := headCommit.Tree()
	if err != nil {
		return nil, err
	}
	return &ProblemInformation{
		headTree.Id().String(),
		headTree.EntryByName("interactive") != nil,
	}, nil
}

func (input *graderBaseInput) Verify() error {
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

func (input *graderBaseInput) getStoredHash() (string, error) {
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

func (input *graderBaseInput) getStoredLength() (int64, error) {
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

func (input *graderBaseInput) Delete() error {
	os.Remove(fmt.Sprintf("%s.tmp", input.archivePath))
	os.Remove(fmt.Sprintf("%s.sha1", input.archivePath))
	os.Remove(fmt.Sprintf("%s.len", input.archivePath))
	return os.Remove(input.archivePath)
}

// Transmit sends a serialized version of the Input to the runner. It sends a
// .tar.gz file with the Content-SHA1 header with the hexadecimal
// representation of its SHA-1 hash.
func (input *graderBaseInput) Transmit(w http.ResponseWriter) error {
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

// graderInput is an Input generated from a git repository that is then stored
// in a .tar.gz file that can be sent to a runner.
type graderInput struct {
	graderBaseInput
	repositoryPath string
	problemName    string
	loader         *SettingsLoader
}

func (input *graderInput) Persist() error {
	if err := os.MkdirAll(path.Dir(input.archivePath), 0755); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp", input.archivePath)
	defer os.Remove(tmpPath)
	settings, uncompressedSize, err := CreateArchiveFromGit(
		input.problemName,
		tmpPath,
		input.repositoryPath,
		input.Hash(),
		input.loader,
	)
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

	*input.Settings() = *settings
	input.storedHash = fmt.Sprintf("%0x", hash)
	input.uncompressedSize = uncompressedSize
	input.Commit(stat.Size())
	return nil
}

func getLibinteractiveSettings(
	contents []byte,
	moduleName string,
	parentLang string,
) (*common.InteractiveSettings, error) {
	cmd := exec.Command(
		"/usr/bin/java",
		"-jar", "/usr/share/java/libinteractive.jar",
		"json",
		"--module-name", moduleName,
		"--parent-lang", parentLang,
		"--omit-debug-targets",
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go (func() {
		stdin.Write(contents)
		stdin.Close()
	})()
	settings := common.InteractiveSettings{}
	if err := json.NewDecoder(stdout).Decode(&settings); err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}
	return &settings, nil
}

func parseTestplan(contents string) (map[string]*big.Rat, error) {
	rawCaseWeights := make(map[string]*big.Rat)
	testplanRe := regexp.MustCompile(`^\s*([^# \t]+)\s+([0-9.]+).*$`)
	for _, line := range strings.Split(contents, "\n") {
		m := testplanRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var err error
		rawCaseWeights[m[1]], err = common.ParseRational(m[2])
		if err != nil {
			return nil, err
		}
	}
	return rawCaseWeights, nil
}

// CreateArchiveFromGit creates an archive that can be sent to a Runner as an
// Input from a git repository.
func CreateArchiveFromGit(
	problemName string,
	archivePath string,
	repositoryPath string,
	inputHash string,
	loader *SettingsLoader,
) (*common.ProblemSettings, int64, error) {
	settings := loader.Settings
	if settings.Validator.Name == "token-numeric" {
		tolerance := 1e-6
		settings.Validator.Tolerance = &tolerance
	}

	repository, err := git.OpenRepository(repositoryPath)
	if err != nil {
		return nil, 0, err
	}
	defer repository.Free()

	treeOid, err := git.NewOid(loader.GitTree)
	if err != nil {
		return nil, 0, err
	}

	tree, err := repository.LookupTree(treeOid)
	if err != nil {
		return nil, 0, err
	}
	defer tree.Free()
	odb, err := repository.Odb()
	if err != nil {
		return nil, 0, err
	}
	defer odb.Free()

	tmpFd, err := os.Create(archivePath)
	if err != nil {
		return nil, 0, err
	}
	defer tmpFd.Close()

	gz := gzip.NewWriter(tmpFd)
	defer gz.Close()

	archive := tar.NewWriter(gz)
	defer archive.Close()

	var walkErr error
	var uncompressedSize int64
	var rawCaseWeights map[string]*big.Rat
	var hasTestPlan bool
	var libinteractiveIdlContents []byte
	var libinteractiveModuleName string
	var libinteractiveParentLang string
	if testplanEntry := tree.EntryByName("testplan"); testplanEntry != nil && testplanEntry.Type == git.ObjectBlob {
		blob, err := repository.LookupBlob(testplanEntry.Id)
		if err != nil {
			return nil, 0, err
		}
		defer blob.Free()
		rawCaseWeights, err = parseTestplan(string(blob.Contents()))
		if err != nil {
			return nil, 0, err
		}
		hasTestPlan = true
	} else {
		rawCaseWeights = make(map[string]*big.Rat)
	}
	tree.Walk(func(parent string, entry *git.TreeEntry) int {
		untrimmedPath := path.Join(parent, entry.Name)
		if strings.HasPrefix(untrimmedPath, "interactive/") {
			if strings.HasSuffix(untrimmedPath, ".idl") &&
				entry.Type == git.ObjectBlob {
				var blob *git.Blob
				blob, walkErr = repository.LookupBlob(entry.Id)
				if walkErr != nil {
					return -1
				}
				defer blob.Free()
				libinteractiveIdlContents = blob.Contents()
				libinteractiveModuleName = strings.TrimSuffix(entry.Name, ".idl")
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
				if _, walkErr = archive.Write(libinteractiveIdlContents); walkErr != nil {
					return -1
				}
			} else if strings.HasPrefix(entry.Name, "Main.") &&
				!strings.HasPrefix(entry.Name, "Main.distrib.") &&
				entry.Type == git.ObjectBlob {
				var blob *git.Blob
				blob, walkErr = repository.LookupBlob(entry.Id)
				if walkErr != nil {
					return -1
				}
				defer blob.Free()
				libinteractiveParentLang = strings.TrimPrefix(entry.Name, "Main.")
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
				if _, walkErr = archive.Write(blob.Contents()); walkErr != nil {
					return -1
				}
			}
			return 0
		}
		if strings.HasPrefix(untrimmedPath, "validator.") &&
			settings.Validator.Name == "custom" &&
			entry.Type == git.ObjectBlob {
			lang := strings.Trim(filepath.Ext(untrimmedPath), ".")
			settings.Validator.Lang = &lang
			var blob *git.Blob
			blob, walkErr = repository.LookupBlob(entry.Id)
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
			if _, walkErr = archive.Write(blob.Contents()); walkErr != nil {
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
				// If a test plan is present, it should mention all cases.
				if hasTestPlan {
					walkErr = fmt.Errorf("Case not found in testplan: %s", caseName)
					return -1
				}
				rawCaseWeights[caseName] = big.NewRat(1, 1)
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
			var blob *git.Blob
			blob, walkErr = repository.LookupBlob(entry.Id)
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
				if _, walkErr = io.Copy(archive, stream); walkErr != nil {
					return -1
				}
			} else {
				// That particular object cannot be streamed. Allocate the blob in
				// memory and write it to the archive.
				if _, walkErr = archive.Write(blob.Contents()); walkErr != nil {
					return -1
				}
			}
		}
		return 0
	})
	if walkErr != nil {
		return nil, 0, walkErr
	}

	// Generate the group/case settings.
	cases := make(map[string][]common.CaseSettings)
	totalWeight := &big.Rat{}
	for _, weight := range rawCaseWeights {
		totalWeight.Add(totalWeight, weight)
	}
	for caseName, weight := range rawCaseWeights {
		components := strings.SplitN(caseName, ".", 2)
		groupName := components[0]
		if _, ok := cases[groupName]; !ok {
			cases[groupName] = make([]common.CaseSettings, 0)
		}
		cases[groupName] = append(cases[groupName], common.CaseSettings{
			Name:   caseName,
			Weight: common.RationalDiv(weight, totalWeight),
		})
	}
	settings.Cases = make([]common.GroupSettings, 0)
	for groupName, cases := range cases {
		sort.Sort(common.ByCaseName(cases))
		settings.Cases = append(settings.Cases, common.GroupSettings{
			Cases: cases,
			Name:  groupName,
		})
	}
	sort.Sort(common.ByGroupName(settings.Cases))

	if libinteractiveIdlContents != nil && libinteractiveParentLang != "" {
		settings.Interactive, err = getLibinteractiveSettings(
			libinteractiveIdlContents,
			libinteractiveModuleName,
			libinteractiveParentLang,
		)
		if err != nil {
			return nil, 0, err
		}
	}

	// Finally, write settings.json.
	settingsBlob, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return nil, 0, err
	}
	hdr := &tar.Header{
		Name:     "settings.json",
		Typeflag: tar.TypeReg,
		Mode:     0644,
		Size:     int64(len(settingsBlob)),
	}
	uncompressedSize += int64(len(settingsBlob))
	if err = archive.WriteHeader(hdr); err != nil {
		return nil, 0, err
	}
	if _, err = archive.Write(settingsBlob); err != nil {
		return nil, 0, err
	}

	return settings, uncompressedSize, nil
}

// inputFactory is an InputFactory that can store specific versions of a
// problem's git repository into a .tar.gz file that can be easily shipped to
// runners.
type inputFactory struct {
	problemName string
	config      *common.Config
	loader      *SettingsLoader
}

// NewInputFactory returns a new InputFactory.
func NewInputFactory(
	problemName string,
	config *common.Config,
	loader *SettingsLoader,
) common.InputFactory {
	return &inputFactory{
		problemName: problemName,
		config:      config,
		loader:      loader,
	}
}

// NewInput returns an Input that corresponds to the specified hash.
func (factory *inputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &graderInput{
		graderBaseInput: graderBaseInput{
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
		loader:      factory.loader,
		problemName: factory.problemName,
	}
}
