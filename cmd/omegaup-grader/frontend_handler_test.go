package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	base "github.com/omegaup/go-base/v3"
	"github.com/omegaup/quark/broadcaster"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
)

func newInMemoryDB(t *testing.T, scoreMode string) *sql.DB {
	t.Helper()
	db, err := sql.Open(
		"sqlite3",
		":memory:",
	)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	if _, err := execWithRetry(
		db,
		`
		CREATE TABLE Identities (
			identity_id INTEGER PRIMARY KEY AUTOINCREMENT,
			username varchar NOT NULL UNIQUE,
			password varchar DEFAULT NULL,
			name varchar DEFAULT NULL,
			user_id int DEFAULT NULL,
			language_id int DEFAULT NULL,
			country_id varchar DEFAULT NULL,
			state_id varchar DEFAULT NULL,
			gender varchar DEFAULT NULL,
			current_identity_school_id int DEFAULT NULL
		);
		CREATE TABLE Runs (
			run_id INTEGER PRIMARY KEY AUTOINCREMENT,
			submission_id int NOT NULL,
			version varchar NOT NULL,
			`+"`commit`"+` varchar NOT NULL,
			status varchar NOT NULL DEFAULT 'new',
			verdict varchar NOT NULL,
			runtime int NOT NULL DEFAULT '0',
			penalty int NOT NULL DEFAULT '0',
			memory int NOT NULL DEFAULT '0',
			score double NOT NULL DEFAULT '0',
			contest_score double DEFAULT NULL,
			time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			judged_by varchar DEFAULT NULL,
			UNIQUE (submission_id, version)
		);
		CREATE TABLE Runs_Groups (
			case_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
			run_id int NOT NULL,
			group_name varchar NOT NULL,
			score double NOT NULL DEFAULT '0',
			verdict varchar NOT NULL,
			UNIQUE (run_id,group_name)
		);
		CREATE TABLE Submissions (
			submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
			current_run_id int DEFAULT NULL,
			identity_id int NOT NULL,
			problem_id int NOT NULL,
			problemset_id int DEFAULT NULL,
			guid varchar NOT NULL UNIQUE,
			language varchar NOT NULL,
			time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			status VARCHAR NOT NULL DEFAULT 'new',
			verdict VARCHAR NOT NULL,
			submit_delay int NOT NULL DEFAULT '0',
			type varchar DEFAULT 'normal',
			school_id int DEFAULT NULL
		);
		CREATE TABLE Problems (
			problem_id INTEGER PRIMARY KEY AUTOINCREMENT,
			acl_id int NOT NULL,
			visibility int NOT NULL DEFAULT '1',
			title varchar NOT NULL,
			alias varchar NOT NULL UNIQUE,
			`+"`commit`"+` varchar NOT NULL DEFAULT 'published',
			current_version varchar NOT NULL,
			languages varchar NOT NULL DEFAULT 'c11-gcc,c11-clang,cpp11-11-clang,cpp17-gcc,cpp17-clang,java,py2,py3,rb,cs,pas,hs,lua',
			input_limit int NOT NULL DEFAULT '10240',
			visits int NOT NULL DEFAULT '0',
			submissions int NOT NULL DEFAULT '0',
			accepted int NOT NULL DEFAULT '0',
			difficulty double DEFAULT NULL,
			creation_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			source varchar DEFAULT NULL,
			`+"`order`"+` varchar NOT NULL DEFAULT 'normal',
			deprecated tinyint(1) NOT NULL DEFAULT '0',
			email_clarifications tinyint(1) NOT NULL DEFAULT '0',
			quality double DEFAULT NULL,
			quality_histogram text,
			difficulty_histogram text,
			quality_seal tinyint(1) NOT NULL DEFAULT '0',
			show_diff varchar NOT NULL DEFAULT 'none',
			allow_user_add_tags tinyint(1) NOT NULL DEFAULT '1'
		);
		CREATE TABLE Contests (
			contest_id INTEGER PRIMARY KEY AUTOINCREMENT,
			problemset_id int NOT NULL,
			acl_id int NOT NULL,
			title varchar(256) NOT NULL,
			description tinytext NOT NULL,
			start_time timestamp NOT NULL DEFAULT '2000-01-01 06:00:00',
			finish_time timestamp NOT NULL DEFAULT '2000-01-01 06:00:00',
			last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			window_length int DEFAULT NULL,
			rerun_id int NOT NULL,
			admission_mode varchar NOT NULL DEFAULT 'private',
			alias varchar(32) NOT NULL UNIQUE,
			scoreboard int NOT NULL DEFAULT '1',
			points_decay_factor double NOT NULL DEFAULT '0',
			score_mode varchar NOT NULL DEFAULT 'partial',
			submissions_gap int NOT NULL DEFAULT '60',
			feedback varchar NOT NULL DEFAULT 'none',
			penalty int NOT NULL DEFAULT '1',
			penalty_type varchar NOT NULL,
			penalty_calc_policy varchar NOT NULL,
			show_scoreboard_after tinyint(1) NOT NULL DEFAULT '1',
			urgent tinyint(1) NOT NULL DEFAULT '0',
			languages varchar DEFAULT NULL,
			recommended tinyint(1) NOT NULL DEFAULT '0'
		);
		CREATE TABLE Problemsets (
			problemset_id INTEGER PRIMARY KEY AUTOINCREMENT,
			acl_id int NOT NULL,
			access_mode varchar NOT NULL DEFAULT 'public',
			languages varchar DEFAULT NULL,
			needs_basic_information tinyint(1) NOT NULL DEFAULT '0',
			requests_user_information varchar NOT NULL DEFAULT 'no',
			scoreboard_url varchar(30) NOT NULL,
			scoreboard_url_admin varchar(30) NOT NULL,
			type varchar NOT NULL DEFAULT 'Contest',
			contest_id int DEFAULT NULL,
			assignment_id int DEFAULT NULL,
			interview_id int DEFAULT NULL,
			UNIQUE (problemset_id,contest_id,assignment_id,interview_id)
		);
		CREATE TABLE Problemset_Problems (
			problemset_id NOT NULL,
			problem_id int NOT NULL,
			`+"`commit`"+` varchar NOT NULL DEFAULT 'published',
			version varchar NOT NULL,
			points double NOT NULL DEFAULT '1',
			`+"`order`"+` int NOT NULL DEFAULT '1'
		);

		INSERT INTO Problemsets (
			problemset_id, acl_id, scoreboard_url, scoreboard_url_admin
		) VALUES (
			1, 1, "", ""
		);
		INSERT INTO Contests (
			contest_id, problemset_id, acl_id, alias, title, description, rerun_id,
			score_mode, penalty_type, penalty_calc_policy, start_time
		) VALUES (
			1, 1, 1, "contest", "Contest", "Contest", 0, ?, "none", "sum",
			"1970-01-01 00:00:00"
		);
		INSERT INTO Problemset_Problems (
			problemset_id, problem_id, `+"`commit`"+`, version
		) VALUES (
			1, 1, "1", "1"
		);
		INSERT INTO Problems (
			problem_id, acl_id, title, alias, `+"`commit`"+`, current_version
		) VALUES (
			1, 1, "Problem", "problem", "1", "1"
		);
		INSERT INTO Submissions (
			submission_id, current_run_id, identity_id, problem_id, guid, language,
			time, status, verdict
		) VALUES (
			1, 1, 1, 1, "1", "py3", "1970-01-01 00:00:00", "new", "JE"
		);
		INSERT INTO Runs (
			run_id, submission_id, version, `+"`commit`"+`, verdict, time
		) VALUES (
			1, 1, "1", "1", "JE", "1970-01-01 00:00:00"
		);
		INSERT INTO Runs_Groups (
			case_run_id, run_id, group_name, score, verdict
		) VALUES (
			1, 1, "easy", 0.1, "PA"
		);
		INSERT INTO Runs_Groups (
			case_run_id, run_id, group_name, score, verdict
		) VALUES (
			2, 1, "medium", 0.2, "PA"
		);
		INSERT INTO Runs_Groups (
			case_run_id, run_id, group_name, score, verdict
		) VALUES (
			3, 1, "sample", 0.3, "PA"
		);
		INSERT INTO Identities (
			identity_id, username
		) VALUES (
			1, "identity"
		);
	`, scoreMode); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	return db
}

func TestUpdateDatabase(t *testing.T) {
	ctx := newGraderContext(t)
	db := newInMemoryDB(t, "partial")

	var count int
	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Runs WHERE verdict = "AC";`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 0 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 0)
	}

	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Runs_Groups;`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 3 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 0)
	}

	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Submissions WHERE verdict = "AC";`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 0 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 0)
	}

	run := grader.RunInfo{
		ID:           1,
		SubmissionID: 1,
		GUID:         "1",
		Run:          &common.Run{},
		PenaltyType:  "none",
		Result: runner.RunResult{
			Verdict:      "AC",
			Score:        big.NewRat(1, 1),
			ContestScore: big.NewRat(1, 1),
			MaxScore:     big.NewRat(1, 1),
			Time:         1.,
			WallTime:     1.,
			Memory:       base.Mebibyte,
			JudgedBy:     "Test",
			Groups: []runner.GroupResult{
				{
					Group:        "group1",
					Score:        big.NewRat(1, 2),
					ContestScore: big.NewRat(1, 2),
					MaxScore:     big.NewRat(1, 2),
					Cases: []runner.CaseResult{
						{
							Verdict:      "AC",
							Name:         "1",
							Score:        big.NewRat(1, 1),
							ContestScore: big.NewRat(1, 2),
							MaxScore:     big.NewRat(1, 2),
						},
					},
				},
				{
					Group:        "group2",
					Score:        big.NewRat(1, 2),
					ContestScore: big.NewRat(1, 2),
					MaxScore:     big.NewRat(1, 2),
					Cases: []runner.CaseResult{
						{
							Verdict:      "AC",
							Name:         "2",
							Score:        big.NewRat(1, 1),
							ContestScore: big.NewRat(1, 2),
							MaxScore:     big.NewRat(1, 2),
						},
					},
				},
			},
		},
	}
	if err := updateDatabase(ctx, db, "ready", &run); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}

	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Runs WHERE verdict = "AC";`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 1 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 1)
	}
	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Runs_Groups;`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 5 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 2)
	}
	if err := queryRowWithRetry(
		db,
		`SELECT COUNT(*) FROM Submissions WHERE verdict = "AC";`,
	).Scan(
		&count,
	); err != nil {
		t.Fatalf("Error updating the database: %v", err)
	}
	if count != 1 {
		t.Errorf("Wrong number of rows in the database. found %v, want %v", count, 1)
	}
}

func TestBroadcastRun(t *testing.T) {
	ctx := newGraderContext(t)
	scenarios := []struct {
		scoreMode     string
		verdict       string
		score         *big.Rat
		expectedScore float64
		scoreByGroup  map[string]any
	}{
		{
			scoreMode:     "partial",
			verdict:       "AC",
			score:         big.NewRat(1, 1),
			expectedScore: 1.,
			scoreByGroup: map[string]any{
				"easy":   0.1,
				"medium": 0.2,
				"sample": 0.3,
			},
		},
		{
			scoreMode:     "partial",
			verdict:       "PA",
			score:         big.NewRat(1, 2),
			expectedScore: 0.5,
			scoreByGroup: map[string]any{
				"easy":   0.1,
				"medium": 0.2,
				"sample": 0.3,
			},
		},
		{
			scoreMode:     "all_or_nothing",
			verdict:       "PA",
			score:         big.NewRat(1, 2),
			expectedScore: 0.,
			scoreByGroup: map[string]any{
				"easy":   0.1,
				"medium": 0.2,
				"sample": 0.3,
			},
		},
		{
			scoreMode:     "max_per_group",
			verdict:       "PA",
			score:         big.NewRat(1, 3),
			expectedScore: 0.3333333333333333,
			scoreByGroup: map[string]any{
				"easy":   0.1,
				"medium": 0.2,
				"sample": 0.3,
			},
		},
	}
	for idx, s := range scenarios {
		t.Run(fmt.Sprintf("%d: verdict=%s scoreMode=%s", idx, s.verdict, s.scoreMode), func(t *testing.T) {
			db := newInMemoryDB(t, s.scoreMode)

			var message broadcaster.Message
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				decoder := json.NewDecoder(r.Body)
				defer r.Body.Close()
				if err := decoder.Decode(&message); err != nil {
					t.Fatalf("Failed to read request from client: %v", err)
				}

				w.Write([]byte(`{"status": "ok"}`))
			}))
			ctx.Config.Grader.BroadcasterURL = ts.URL
			defer ts.Close()

			run := grader.RunInfo{
				ID:          1,
				GUID:        "1",
				Run:         &common.Run{},
				PenaltyType: "none",
				ScoreMode:   s.scoreMode,
				Result: runner.RunResult{
					Verdict:      s.verdict,
					Score:        s.score,
					ContestScore: s.score,
					MaxScore:     s.score,
					Time:         1.,
					WallTime:     1.,
					Memory:       base.Mebibyte,
					JudgedBy:     "Test",
				},
			}
			if err := broadcastRun(ctx, db, ts.Client(), &run); err != nil {
				t.Fatalf("Error broadcasting run: %v", err)
			}

			var encodedMessage map[string]any
			if err := json.Unmarshal([]byte(message.Message), &encodedMessage); err != nil {
				t.Fatalf("Error decoding inner message: %v", err)
			}

			runInfo, ok := encodedMessage["run"].(map[string]any)
			if !ok {
				t.Fatalf("Message does not contain a run entry: %v", encodedMessage)
			}

			if !reflect.DeepEqual(s.scoreByGroup, runInfo["score_by_group"]) {
				t.Errorf("message.score_by_group=%v (type %T), want %v (type %T)",
					runInfo["score_by_group"], runInfo["score_by_group"],
					s.scoreByGroup, s.scoreByGroup)
			}

			for key, value := range map[string]any{
				"guid":          "1",
				"status":        "ready",
				"verdict":       s.verdict,
				"username":      "identity",
				"score":         s.expectedScore,
				"contest_score": s.expectedScore,
			} {
				if runInfo[key] != value {
					t.Errorf("message.%s=%v, want %v", key, runInfo[key], value)
				}
			}
		})
	}
}
