package queue

import (
	"database/sql"
	"fmt"
	"github.com/omegaup/quark/context"
	"math/rand"
	"path"
	"sync/atomic"
	"time"
)

var (
	runID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	runID = uint64(r.Int63())
}

type Problem struct {
	Name   string
	Hash   string
	Points *float64
}

type Run struct {
	ID       uint64
	GUID     string
	Contest  *string
	Language string
	Problem  Problem
}

type RunContext struct {
	Run   *Run
	Input *Input
}

func newRunID() uint64 {
	return atomic.AddUint64(&runID, 1)
}

func (run *Run) GetRepositoryPath(config *context.Config) string {
	return path.Join(config.Grader.RuntimePath, "problems.git", run.Problem.Name)
}

func (run *Run) GetInputPath(config *context.Config) string {
	return path.Join(config.Grader.RuntimePath, "cache",
		fmt.Sprintf("%s.tar.gz", run.Problem.Hash))
}

func NewRunContext(id int64, context *context.Context) (*RunContext, error) {
	run := &Run{
		ID: newRunID(),
	}
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := context.DB.QueryRow(
		`SELECT
			s.guid, c.alias, s.language, p.alias, pv.hash, cp.points
		FROM
			Submissions s
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		INNER JOIN
			Problem_Versions pv ON pv.version_id = p.current_version
		LEFT JOIN
			Contests c ON c.contest_id = s.contest_id
		LEFT JOIN
			Contest_Problems cp ON cp.problem_id = s.problem_id AND
			cp.contest_id = s.contest_id
		WHERE
			s.submission_id = ?;`, id).Scan(
		&run.GUID, &contestName, &run.Language, &run.Problem.Name,
		&run.Problem.Hash, &contestPoints)
	if err != nil {
		return nil, err
	}
	if contestName.Valid {
		run.Contest = &contestName.String
	}
	if contestPoints.Valid {
		run.Problem.Points = &contestPoints.Float64
	}

	input, err := DefaultCodeManager.Get(run)
	if err != nil {
		return nil, err
	}

	runctx := &RunContext{
		Run:   run,
		Input: input,
	}
	return runctx, nil
}
