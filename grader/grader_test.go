package grader

import (
	"bytes"
	"errors"
	"fmt"
	git "gopkg.in/libgit2/git2go.v22"
	"io/ioutil"
	"os"
	"path"
	"time"
)

func newGraderContext() (*Context, error) {
	dirname, err := ioutil.TempDir("/tmp", "gradertest")
	if err != nil {
		return nil, err
	}
	ctx, err := NewContext(bytes.NewBufferString(
		fmt.Sprintf(
			"{"+
				"\"Logging\": {\"File\": \"stderr\"}, "+
				"\"Tracing\": {\"Enabled\": false}, "+
				"\"InputManager\": {\"CacheSize\": 1024}, "+
				"\"Grader\": {\"RuntimePath\": %q}, "+
				"\"DB\": {\"Driver\": \"sqlite3\", \"DataSourceName\": \":memory:\"}"+
				"}",
			dirname,
		),
	))
	if err != nil {
		return nil, err
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

	// Setting up the database.
	statements := []string{
		`CREATE TABLE Contests (
			contest_id INT PRIMARY_KEY,
			alias VARCHAR(40) NOT NULL
		);`,
		`CREATE TABLE Contest_Problems (
			contest_id INT NOT NULL,
			problem_id INT NOT NULL,
			points FLOAT NOT NULL
		);`,
		`CREATE TABLE Problems (
			problem_id INT PRIMARY_KEY,
			alias VARCHAR(40) NOT NULL,
			current_version INT NOT NULL
		);`,
		`CREATE TABLE Problem_Versions (
			version_id INT PRIMARY_KEY,
			problem_id INT NOT NULL,
			hash CHAR(40) NOT NULL
		);`,
		`CREATE TABLE Submissions (
			submission_id INT PRIMARY_KEY,
			problem_id INT NOT NULL,
			contest_id INT NULL,
			guid CHAR(40) NOT NULL,
			language CHAR(4) NOT NULL
		);`,
		`CREATE TABLE Runs (
			run_id INT PRIMARY KEY,
			submission_id INT NOT NULL,
			version_id INT NOT NULL,
			verdict CHAR(3) NOT NULL,
			score FLOAT NOT NULL,
			contest_score FLOAT
		);`,
		`INSERT INTO Problems VALUES(1, 'test', 2);`,
		`INSERT INTO Problem_Versions VALUES(
			1, 1, '6b70f47f4f4d57fcac0ac91ed0c6320218764236'
		);`,
		`INSERT INTO Problem_Versions VALUES(
			2, 1, '2af3227d22470f4d9730937b6b47fd79622fdb32'
		);`,
		`INSERT INTO Contests VALUES(1, 'test');`,
		`INSERT INTO Contest_Problems VALUES(1, 1, 100.0);`,
		`INSERT INTO Submissions VALUES(
			1, 1, NULL, '92d8c5a0eceef3c05f4149fc04b62bb2cd50d9c6', 'py'
		);`,
		`INSERT INTO Submissions VALUES(
			2, 1, 1, 'c1b00dfe44413ca3dc845aa0f448c878d96e941e', 'py'
		);`,
		`INSERT INTO Runs VALUES(1, 1, 1, 'JE', 0, NULL);`,
		`INSERT INTO Runs VALUES(2, 1, 2, 'JE', 0, NULL);`,
		`INSERT INTO Runs VALUES(3, 2, 1, 'JE', 0, NULL);`,
		`INSERT INTO Runs VALUES(4, 2, 2, 'JE', 0, NULL);`,
	}
	for _, statement := range statements {
		if _, err := ctx.DB.Exec(statement); err != nil {
			return nil, err
		}
	}

	// Setting up files.
	dirs := []string{
		"submissions/92",
		"submissions/c1",
		"grader",
		"cache",
		"problems.git/test",
	}
	for _, d := range dirs {
		if err := os.MkdirAll(path.Join(dirname, d), 0755); err != nil {
			return nil, err
		}
	}
	files := []struct {
		filename, contents string
	}{
		{
			"submissions/92/d8c5a0eceef3c05f4149fc04b62bb2cd50d9c6",
			"print 3",
		},
		{
			"submissions/c1/b00dfe44413ca3dc845aa0f448c878d96e941e",
			"print sum(map(int, raw_input().strip().split()))",
		},
	}
	for _, ft := range files {
		if err := ioutil.WriteFile(
			path.Join(dirname, ft.filename),
			[]byte(ft.contents),
			0644,
		); err != nil {
			return nil, err
		}
	}

	// Git repository for problem.
	repo, err := git.InitRepository(path.Join(dirname, "problems.git/test"), true)
	if err != nil {
		return nil, err
	}
	odb, err := repo.Odb()
	if err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation("Etc/UTC")
	if err != nil {
		return nil, err
	}
	sig := &git.Signature{
		Name:  "Rand Om Hacker",
		Email: "random@hacker.com",
		When:  time.Date(2015, 11, 30, 03, 01, 0, 0, loc),
	}
	idx, err := repo.Index()
	if err != nil {
		return nil, err
	}
	treeId, err := idx.WriteTree()
	if err != nil {
		return nil, err
	}

	message := "Initial commit\n"
	tree, err := repo.LookupTree(treeId)
	if err != nil {
		return nil, err
	}
	if _, err := repo.CreateCommit("HEAD", sig, sig, message, tree); err != nil {
		return nil, err
	}
	type filecontents struct {
		path, contents string
	}
	gitcommits := []struct {
		message      string
		expectedhash string
		files        []filecontents
	}{
		{
			"first commit\n",
			"6b70f47f4f4d57fcac0ac91ed0c6320218764236",
			[]filecontents{
				{"cases/in/0.in", "1 2"},
				{"cases/out/0.out", "3"},
				{"cases/settings.json", `{
  "Cases": [
		{"Cases": [{"Name": "0", "Weight": 1.0}], "Name": "0", "Weight": 1.0}
  ], 
  "Limits": {
    "ExtraWallTime": 0, 
    "MemoryLimit": 67108864, 
    "OutputLimit": 16384, 
    "OverallWallTimeLimit": 60000, 
    "TimeLimit": 3000, 
    "ValidatorTimeLimit": 3000
  }, 
  "Slow": false, 
	"Validator": {"Name": "token-numeric", "Tolerance": 0.001}
}`},
			},
		},
		{
			"second commit\n",
			"2af3227d22470f4d9730937b6b47fd79622fdb32",
			[]filecontents{
				{"cases/in/0.in", "1 2"},
				{"cases/out/0.out", "3"},
				{"cases/in/1.in", "2 3"},
				{"cases/out/1.out", "5"},
				{"cases/settings.json", `{
  "Cases": [
		{"Cases": [{"Name": "0", "Weight": 0.5}], "Name": "0", "Weight": 0.5}, 
		{"Cases": [{"Name": "1", "Weight": 0.5}], "Name": "1", "Weight": 0.5}
  ], 
  "Limits": {
    "ExtraWallTime": 0, 
    "MemoryLimit": 67108864, 
    "OutputLimit": 16384, 
    "OverallWallTimeLimit": 60000, 
    "TimeLimit": 3000, 
    "ValidatorTimeLimit": 3000
  }, 
  "Slow": false, 
	"Validator": {"Name": "token-numeric", "Tolerance": 0.001}
}`},
			},
		},
	}
	for _, gct := range gitcommits {
		for _, gft := range gct.files {
			oid, err := odb.Write([]byte(gft.contents), git.ObjectBlob)
			if err != nil {
				return nil, err
			}
			if err := idx.Add(&git.IndexEntry{
				Mode: git.FilemodeBlob,
				Size: uint(len(gft.contents)),
				Id:   oid,
				Path: gft.path,
			}); err != nil {
				return nil, err
			}
		}
		treeId, err := idx.WriteTree()
		if err != nil {
			return nil, err
		}

		currentBranch, err := repo.Head()
		if err != nil {
			return nil, err
		}
		currentTip, err := repo.LookupCommit(currentBranch.Target())
		if err != nil {
			return nil, err
		}

		message := gct.message
		tree, err := repo.LookupTree(treeId)
		if err != nil {
			return nil, err
		}
		casesTree, err := tree.EntryByPath("cases")
		if err != nil {
			return nil, err
		}
		if casesTree.Id.String() != gct.expectedhash {
			return nil, errors.New(fmt.Sprintf(
				"expected %q, got %q",
				gct.expectedhash,
				casesTree.Id.String(),
			))
		}
		if _, err := repo.CreateCommit(
			"HEAD", sig, sig, message, tree, currentTip,
		); err != nil {
			return nil, err
		}
	}

	return ctx, nil
}
