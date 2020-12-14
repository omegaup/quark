package grader

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	git "github.com/lhchavez/git2go/v32"
)

const (
	headCommit = "d129ffad215c8c87d2e646b959f31e5f279c1cff"
)

func newGraderContext(t *testing.T) (*Context, error) {
	dirname, err := ioutil.TempDir("/tmp", t.Name())
	if err != nil {
		return nil, err
	}
	ctx, err := NewContext(bytes.NewBufferString(
		fmt.Sprintf(
			"{"+
				"\"Logging\": {\"File\": \"stderr\"}, "+
				"\"Tracing\": {\"Enabled\": false}, "+
				"\"InputManager\": {\"CacheSize\": 1024}, "+
				"\"Grader\": {"+
				"  \"RuntimePath\": %q, "+
				"  \"Ephemeral\": {\"EphemeralSizeLimit\": 1024}"+
				"}"+
				"}",
			dirname,
		),
	))
	if err != nil {
		return nil, err
	}
	ctx.Config.Runner.PreserveFiles = os.Getenv("PRESERVE") != ""

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
	treeID, err := idx.WriteTree()
	if err != nil {
		return nil, err
	}

	message := "Initial commit\n"
	tree, err := repo.LookupTree(treeID)
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
			"7c26e62a4c982713463d4c0334c6793840291b0e",
			[]filecontents{
				{"cases/0.in", "1 2"},
				{"cases/0.out", "3"},
				{"settings.json", `{
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
			headCommit,
			[]filecontents{
				{"cases/0.in", "1 2"},
				{"cases/0.out", "3"},
				{"cases/1.in", "2 3"},
				{"cases/1.out", "5"},
				{"settings.json", `{
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
				Size: uint32(len(gft.contents)),
				Id:   oid,
				Path: gft.path,
			}); err != nil {
				return nil, err
			}
		}
		treeID, err := idx.WriteTree()
		if err != nil {
			return nil, err
		}

		currentBranch, err := repo.Head()
		if err != nil {
			return nil, err
		}
		defer currentBranch.Free()
		currentTip, err := repo.LookupCommit(currentBranch.Target())
		if err != nil {
			return nil, err
		}
		defer currentTip.Free()

		message := gct.message
		casesTree, err := repo.LookupTree(treeID)
		if err != nil {
			return nil, err
		}
		defer casesTree.Free()
		if casesTree.Id().String() != gct.expectedhash {
			return nil, fmt.Errorf(
				"expected %q, got %q",
				gct.expectedhash,
				casesTree.Id().String(),
			)
		}
		if _, err := repo.CreateCommit(
			"HEAD", sig, sig, message, tree, currentTip,
		); err != nil {
			return nil, err
		}
	}

	return ctx, nil
}
