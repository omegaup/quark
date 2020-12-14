package common

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	git "github.com/lhchavez/git2go/v32"
	"github.com/pkg/errors"
)

var (
	// TopLevelEntryNames is the list of top-level files that are found in a .zip file.
	TopLevelEntryNames = []string{
		// public branch:
		"statements",
		"examples",

		// protected branch:
		"solution",
		"tests",

		// private branch:
		"cases",
		"settings.json",
		"testplan",

		// public/private:
		"interactive",
	}

	casesRegexp = regexp.MustCompile("^cases/([^/]+)\\.in$")
)

// ProblemFiles represents the files of a problem.
type ProblemFiles interface {
	// Stringer returns the name of the ProblemFiles.
	fmt.Stringer

	// Files returns the list of all files.
	Files() []string

	// GetContents returns the contents of a file as a []byte.
	GetContents(path string) ([]byte, error)

	// GetStringContents returns the contents of a file as string.
	GetStringContents(path string) (string, error)

	// Open returns the contents of a file as a ReadCloser.
	Open(path string) (io.ReadCloser, error)

	// Close frees all resources of the ProblemFiles.
	Close() error
}

// filesystemProblemFiles is a ProblemFiles that is backed by a directory in
// the filesystem.
type filesystemProblemFiles struct {
	path  string
	files []string
}

var _ ProblemFiles = &filesystemProblemFiles{}

// String implements the fmt.Stringer interface.
func (f *filesystemProblemFiles) String() string {
	return f.path
}

func (f *filesystemProblemFiles) Files() []string {
	return f.files
}

func (f *filesystemProblemFiles) GetContents(path string) ([]byte, error) {
	r, err := f.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, errors.Wrapf(err, "read %q in %q", path, f.path)
	}

	return buf.Bytes(), nil
}

func (f *filesystemProblemFiles) GetStringContents(path string) (string, error) {
	b, err := f.GetContents(path)
	return string(b), err
}

func (f *filesystemProblemFiles) Open(filepath string) (io.ReadCloser, error) {
	fh, err := os.Open(path.Join(f.path, filepath))
	if err != nil {
		return nil, err
	}
	return fh, nil
}

func (f *filesystemProblemFiles) Close() error {
	return nil
}

// NewProblemFilesFromFilesystem returns a ProblemFiles for a problem backed by
// a directory in the filesystem.
func NewProblemFilesFromFilesystem(
	problemPath string,
) (ProblemFiles, error) {
	f := &filesystemProblemFiles{
		path: problemPath,
	}

	err := filepath.Walk(problemPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			// Skip
			return nil
		}
		rel, err := filepath.Rel(problemPath, path)
		if err != nil {
			return err
		}
		f.files = append(f.files, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(f.files)

	return f, nil
}

type gitProblemFiles struct {
	repo     *git.Repository
	commitID git.Oid
	tree     *git.Tree
	files    []string
}

var _ ProblemFiles = &gitProblemFiles{}

// String implements the fmt.Stringer interface.
func (f *gitProblemFiles) String() string {
	return fmt.Sprintf("%s:%s", f.repo.Path(), f.commitID.String())
}

func (f *gitProblemFiles) Files() []string {
	return f.files
}

func (f *gitProblemFiles) GetContents(path string) ([]byte, error) {
	entry, err := f.tree.EntryByPath(path)
	if err != nil {
		return nil, os.NewSyscallError(
			fmt.Sprintf("open %q in %q: %v", path, f.String(), err),
			os.ErrNotExist,
		)
	}
	obj, err := f.repo.LookupBlob(entry.Id)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"lookup %q in %q",
			path,
			f.String(),
		)
	}
	defer obj.Free()

	return obj.Contents(), nil
}

func (f *gitProblemFiles) GetStringContents(path string) (string, error) {
	b, err := f.GetContents(path)
	return string(b), err
}

func (f *gitProblemFiles) Open(path string) (io.ReadCloser, error) {
	b, err := f.GetContents(path)
	return ioutil.NopCloser(bytes.NewReader(b)), err
}

func (f *gitProblemFiles) Close() error {
	if f.tree != nil {
		f.tree.Free()
		f.tree = nil
	}
	if f.repo != nil {
		f.repo.Free()
		f.repo = nil
	}
	return nil
}

// NewProblemFilesFromGit returns a ProblemFiles for a problem backed by
// a particular commit in a git repository.
func NewProblemFilesFromGit(
	repositoryPath string,
	commitHash string,
) (ProblemFiles, error) {
	grp := &gitProblemFiles{}

	repo, err := git.OpenRepository(repositoryPath)
	if err != nil {
		grp.Close()
		return nil, errors.Wrapf(
			err,
			"failed to open repository %s:%s",
			repositoryPath,
			commitHash,
		)
	}
	grp.repo = repo

	commitID, err := git.NewOid(commitHash)
	if err != nil {
		grp.Close()
		return nil, errors.Wrapf(
			err,
			"failed to parse commit id %s:%s",
			repositoryPath,
			commitHash,
		)
	}
	grp.commitID = *commitID

	commit, err := repo.LookupCommit(commitID)
	if err != nil {
		grp.Close()
		return nil, errors.Wrapf(
			err,
			"failed to lookup commit %s:%s",
			repositoryPath,
			commitHash,
		)
	}
	defer commit.Free()

	tree, err := commit.Tree()
	if err != nil {
		grp.Close()
		return nil, errors.Wrapf(
			err,
			"failed to get tree for commit %s:%s",
			repositoryPath,
			commitHash,
		)
	}
	grp.tree = tree

	err = tree.Walk(func(dirname string, entry *git.TreeEntry) error {
		if entry.Type != git.ObjectBlob {
			// Skip
			return nil
		}
		grp.files = append(grp.files, path.Join(dirname, entry.Name))
		return nil
	})
	if err != nil {
		grp.Close()
		return nil, err
	}
	sort.Strings(grp.files)

	return grp, nil
}

type inMemoryProblemFiles struct {
	contents map[string]string
	name     string
	files    []string
}

var _ ProblemFiles = &inMemoryProblemFiles{}

// String implements the fmt.Stringer interface.
func (f *inMemoryProblemFiles) String() string {
	return f.name
}

func (f *inMemoryProblemFiles) Files() []string {
	return f.files
}

func (f *inMemoryProblemFiles) GetContents(path string) ([]byte, error) {
	s, err := f.GetStringContents(path)
	return []byte(s), err
}

func (f *inMemoryProblemFiles) GetStringContents(path string) (string, error) {
	contents, ok := f.contents[path]
	if !ok {
		return "", os.NewSyscallError(
			fmt.Sprintf("open %q in %q", path, f.String()),
			os.ErrNotExist,
		)
	}
	return contents, nil
}

func (f *inMemoryProblemFiles) Open(path string) (io.ReadCloser, error) {
	s, err := f.GetStringContents(path)
	return ioutil.NopCloser(strings.NewReader(s)), err
}

func (f *inMemoryProblemFiles) Close() error {
	return nil
}

// NewProblemFilesFromMap returns a ProblemFiles for a problem backed by
// map of strings to strings in memory
func NewProblemFilesFromMap(
	contents map[string]string,
	name string,
) ProblemFiles {
	f := &inMemoryProblemFiles{
		contents: contents,
		name:     name,
	}
	for path := range contents {
		f.files = append(f.files, path)
	}
	sort.Strings(f.files)
	return f
}

// zipProblemFiles is a ProblemFiles that is backed by a directory in
// the filesystem.
type zipProblemFiles struct {
	zipReader   *zip.Reader
	path        string
	fileMapping map[string]*zip.File
}

var _ ProblemFiles = &zipProblemFiles{}

// String implements the fmt.Stringer interface.
func (f *zipProblemFiles) String() string {
	return f.path
}

func (f *zipProblemFiles) Files() []string {
	var files []string
	for path := range f.fileMapping {
		files = append(files, path)
	}
	sort.Strings(files)
	return files
}

func (f *zipProblemFiles) GetContents(path string) ([]byte, error) {
	r, err := f.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, errors.Wrapf(err, "read %q in %q", path, f.path)
	}

	return buf.Bytes(), nil
}

func (f *zipProblemFiles) GetStringContents(path string) (string, error) {
	b, err := f.GetContents(path)
	return string(b), err
}

func (f *zipProblemFiles) Open(path string) (io.ReadCloser, error) {
	zipFile, ok := f.fileMapping[path]
	if !ok {
		return nil, os.NewSyscallError(
			fmt.Sprintf("open %q in %q", path, f.String()),
			os.ErrNotExist,
		)
	}
	return zipFile.Open()
}

func (f *zipProblemFiles) Close() error {
	return nil
}

// isTopLevelComponent returns whether a file component (a file or directory
// name) is considered to be a top-level entry in a .zip file.
func isTopLevelComponent(component string) bool {
	for _, entry := range TopLevelEntryNames {
		if entry == component {
			return true
		}
		if strings.HasPrefix(component, "validator.") {
			return true
		}
	}
	return false
}

func getLongestPathPrefix(zipReader *zip.Reader) []string {
	for _, file := range zipReader.File {
		components := strings.Split(path.Clean(file.Name), "/")
		for idx, component := range components {
			// Whenever we see one of these directories, we know we've reached the
			// root of the problem structure.
			if isTopLevelComponent(component) {
				return components[:idx]
			}
		}
	}
	return []string{}
}

func hasPathPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for idx, p := range a {
		if b[idx] != p {
			return false
		}
	}
	return true
}

// NewProblemFilesFromZip returns a ProblemFiles for a problem backed by
// a .zip file.
func NewProblemFilesFromZip(
	zipReader *zip.Reader,
	zipPath string,
) ProblemFiles {
	longestPrefix := getLongestPathPrefix(zipReader)
	fileMapping := make(map[string]*zip.File)
	for _, file := range zipReader.File {
		components := strings.Split(path.Clean(file.Name), "/")
		if !hasPathPrefix(longestPrefix, components) {
			continue
		}

		// We only cares about files.
		if file.FileInfo().IsDir() {
			continue
		}
		trimmedZipfilePath := strings.Join(components[len(longestPrefix):], "/")
		fileMapping[trimmedZipfilePath] = file
	}

	return &zipProblemFiles{
		zipReader:   zipReader,
		path:        zipPath,
		fileMapping: fileMapping,
	}
}

type chainedProblemFiles struct {
	repositories []ProblemFiles
}

var _ ProblemFiles = &chainedProblemFiles{}

// String implements the fmt.Stringer interface.
func (f *chainedProblemFiles) String() string {
	var names []string

	for _, repo := range f.repositories {
		names = append(names, repo.String())
	}

	return fmt.Sprintf("[%s]", strings.Join(names, ","))
}

func (f *chainedProblemFiles) Files() []string {
	var files []string
	seenFiles := make(map[string]struct{})
	for _, repo := range f.repositories {
		for _, path := range repo.Files() {
			if _, ok := seenFiles[path]; ok {
				continue
			}
			seenFiles[path] = struct{}{}
			files = append(files, path)
		}
	}
	sort.Strings(files)
	return files
}

func (f *chainedProblemFiles) GetContents(path string) ([]byte, error) {
	var lastError error
	for _, repo := range f.repositories {
		c, err := repo.GetContents(path)
		if err == nil {
			return c, err
		}
		lastError = err
	}
	return nil, lastError
}

func (f *chainedProblemFiles) GetStringContents(path string) (string, error) {
	var lastError error
	for _, repo := range f.repositories {
		c, err := repo.GetStringContents(path)
		if err == nil {
			return c, err
		}
		lastError = err
	}
	return "", lastError
}

func (f *chainedProblemFiles) Open(path string) (io.ReadCloser, error) {
	var lastError error
	for _, repo := range f.repositories {
		c, err := repo.Open(path)
		if err == nil {
			return c, err
		}
		lastError = err
	}
	return nil, lastError
}

func (f *chainedProblemFiles) Close() error {
	var lastError error
	for _, repo := range f.repositories {
		if err := repo.Close(); err != nil {
			lastError = err
		}
	}
	return lastError
}

// NewProblemFilesFromChain returns a ProblemFiles that tries to return the
// contents of files in the order in which they are provided. Useful for cases
// where a particular file needs to be overridden (e.g. settings.json).
func NewProblemFilesFromChain(
	repositories ...ProblemFiles,
) ProblemFiles {
	return &chainedProblemFiles{
		repositories: repositories,
	}
}

// GetGroupSettingsForProblem returns the cases with their weights in a way
// that can be added to the ProblemSettings.
func GetGroupSettingsForProblem(f ProblemFiles) ([]GroupSettings, error) {
	// Information needed to build ProblemSettings.Cases.
	caseWeightMapping := NewCaseWeightMapping()
	for _, filename := range f.Files() {
		casesMatches := casesRegexp.FindStringSubmatch(filename)
		if casesMatches == nil {
			continue
		}
		caseName := casesMatches[1]

		caseWeightMapping.AddCaseName(caseName, big.NewRat(1, 1), false)
	}
	r, err := f.Open("testplan")
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.Wrap(
			err,
			"failed to open testplan",
		)
	} else if err == nil {
		zipGroupSettings := caseWeightMapping
		caseWeightMapping, err = NewCaseWeightMappingFromTestplan(r)
		r.Close()
		if err != nil {
			return nil, err
		}

		// Validate that the files in the testplan are all present in the cases/ directory.
		if err := zipGroupSettings.SymmetricDiff(caseWeightMapping, "cases/"); err != nil {
			return nil, err
		}
		// ... and viceversa.
		if err := caseWeightMapping.SymmetricDiff(zipGroupSettings, "testplan"); err != nil {
			return nil, err
		}
	}

	return caseWeightMapping.ToGroupSettings(), nil
}
