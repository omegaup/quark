package common

import (
	"database/sql"
	"io/ioutil"
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
	Name   string   `json:"name"`
	Points *float64 `json:"points,omitempty"`
}

type Run struct {
	ID        uint64  `json:"id"`
	GUID      string  `json:"guid"`
	Contest   *string `json:"contest,omitempty"`
	Language  string  `json:"language"`
	InputHash string  `json:"input_hash"`
	Problem   Problem `json:"problem"`
	Source    string  `json:"source"`
}

func newRunID() uint64 {
	return atomic.AddUint64(&runID, 1)
}

func NewRun(id int64, ctx *Context) (*Run, error) {
	run := &Run{
		ID: newRunID(),
	}
	var contestName sql.NullString
	var contestPoints sql.NullFloat64
	err := ctx.DB.QueryRow(
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
		&run.InputHash, &contestPoints)
	if err != nil {
		return nil, err
	}
	if contestName.Valid {
		run.Contest = &contestName.String
	}
	if contestPoints.Valid {
		run.Problem.Points = &contestPoints.Float64
	}
	contents, err := ioutil.ReadFile(path.Join(ctx.Config.Grader.RuntimePath,
		"submissions", run.GUID[:2], run.GUID[2:]))
	if err != nil {
		return nil, err
	}
	run.Source = string(contents)
	return run, nil
}

func (run *Run) UpdateID() uint64 {
	run.ID = newRunID()
	return run.ID
}
