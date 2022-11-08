package grader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/omegaup/quark/common"
)

type atomicFile struct {
	filename string
	f        *os.File
}

// newAtomicFile creates a temporary file that can be eventually renamed to the
// provided file.
func newAtomicFile(filename string) (*atomicFile, error) {
	dir := path.Dir(filename)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.CreateTemp(dir, fmt.Sprintf(".%s~", path.Base(filename)))
	if err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}
	f.Chmod(0o644)
	return &atomicFile{
		filename: filename,
		f:        f,
	}, nil
}

// cleanup closes the atomic file (if it wasn't before) and removes the
// temporary file that lingered.
func (f *atomicFile) cleanup() {
	if f.f == nil {
		return
	}
	f.f.Close()
	os.Remove(f.f.Name())
}

// commit renames the atomicFile into its intended path.
func (f *atomicFile) commit(modTime *time.Time) error {
	released := f.f
	defer os.Remove(released.Name())
	f.f = nil
	err := released.Close()
	if err != nil {
		return fmt.Errorf("close: %w", err)
	}
	err = os.Rename(released.Name(), f.filename)
	if err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func getArtifact(
	ctx *common.Context,
	s3c *s3.S3,
	bucketName string,
	bucketKey string,
	localPath string,
) (io.ReadCloser, error) {
	f, err := os.Open(localPath)
	if errors.Is(err, fs.ErrNotExist) && s3c != nil {
		// Give it one more good try.
		f, err := newAtomicFile(localPath)
		if err != nil {
			return nil, fmt.Errorf("new: %w", err)
		}
		defer f.cleanup()

		input := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(bucketKey),
		}
		obj, err := s3c.GetObjectWithContext(aws.Context(ctx.Context), input)
		if err != nil {
			return nil, fmt.Errorf("get s3://%s/%s: %w", *input.Bucket, *input.Key, err)
		}
		_, err = io.Copy(f.f, obj.Body)
		obj.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read: get s3://%s/%s: %w", *input.Bucket, *input.Key, err)
		}
		err = f.commit(obj.LastModified)
		if err != nil {
			return nil, fmt.Errorf("commit: get s3://%s/%s: %w", *input.Bucket, *input.Key, err)
		}

		return os.Open(localPath)
	}
	return f, err
}

func putArtifact(
	ctx *common.Context,
	s3c *s3.S3,
	bucketName string,
	bucketKey string,
	localPath string,
	r io.Reader,
) error {
	f, err := newAtomicFile(localPath)
	if err != nil {
		return fmt.Errorf("new: %w", err)
	}
	defer f.cleanup()
	n, err := io.Copy(f.f, r)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	_, err = f.f.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	if s3c != nil {
		input := &s3.PutObjectInput{
			Bucket:        aws.String(bucketName),
			Key:           aws.String(bucketKey),
			Body:          f.f,
			ContentLength: aws.Int64(n),
		}
		_, err = s3c.PutObjectWithContext(aws.Context(ctx.Context), input)
		if err != nil {
			return fmt.Errorf("put s3://%s/%s: %w", *input.Bucket, *input.Key, err)
		}
	}

	err = f.commit(nil)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// ArtifactManager is an abstraction around the filesystem. All writes will go
// to both the filesystem and (if it's set) S3, and reads will be attempted
// against the filesystem first and then S3 as fallback.
type ArtifactManager struct {
	s3c *s3.S3

	Submissions SubmissionsArtifacts
}

// NewArtifactManager returns a new ArtifactManager.
func NewArtifactManager(s3c *s3.S3) *ArtifactManager {
	return &ArtifactManager{
		s3c: s3c,
		Submissions: SubmissionsArtifacts{
			s3c: s3c,
		},
	}
}

// Grader returns a wrapper for the grader artifacts.
func (a *ArtifactManager) Grader(ctx *common.Context, runID int64) Artifacts {
	return &graderArtifacts{
		s3c: a.s3c,
		gradeDir: path.Join(
			ctx.Config.Grader.V1.RuntimeGradePath,
			fmt.Sprintf("%02d/%02d/%d", runID%100, (runID%10000)/100, runID),
		),
		bucketPrefix: strconv.FormatInt(runID, 10),
	}
}

func ephemeralTempDir(ctx *common.Context) (name string, err error) {
	ephemeralPath := path.Join(ctx.Config.Grader.RuntimePath, "ephemeral")
	buf := make([]byte, 10)

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10000; i++ {
		rand.Read(buf)
		try := path.Join(ephemeralPath, fmt.Sprintf("%x", buf))
		err = os.Mkdir(try, 0700)
		if os.IsExist(err) {
			continue
		}
		if os.IsNotExist(err) {
			if _, err := os.Stat(ephemeralPath); os.IsNotExist(err) {
				return "", err
			}
		}
		if err == nil {
			name = try
		}
		break
	}

	return
}

func newEphemeralLocalGrader(ctx *common.Context) (*localGraderArtifacts, error) {
	dir, err := ephemeralTempDir(ctx)
	if err != nil {
		return nil, err
	}
	return &localGraderArtifacts{
		gradeDir: dir,
		token:    path.Base(dir),
	}, nil
}

func newDebugLocalGrader() (*localGraderArtifacts, error) {
	dir, err := os.MkdirTemp("", "grade")
	if err != nil {
		return nil, err
	}
	return &localGraderArtifacts{
		gradeDir: dir,
	}, nil
}

// SubmissionsArtifacts is an object that allows interacting with submissions.
type SubmissionsArtifacts struct {
	s3c *s3.S3
}

// GetSource returns the source of a submission, identified by its guid.
func (a *SubmissionsArtifacts) GetSource(ctx *common.Context, guid string) (string, error) {
	submissionKey := path.Join(
		"submissions",
		guid[:2],
		guid[2:],
	)
	r, err := getArtifact(
		ctx,
		a.s3c,
		"omegaup-backup",
		path.Join("omegaup", submissionKey),
		path.Join(ctx.Config.Grader.V1.RuntimePath, submissionKey),
	)
	if err != nil {
		return "", fmt.Errorf("get source %s: %w", guid, err)
	}
	contents, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return "", fmt.Errorf("read source %s: %w", guid, err)
	}
	return string(contents), nil
}

// PutSource writes the source of the submission to the filesystem (and maybe to S3).
func (a *SubmissionsArtifacts) PutSource(ctx *common.Context, guid string, r io.Reader) error {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	backupSubmissionKey := path.Join(
		"submissions",
		guid[:2],
		guid[2:],
	)
	err = putArtifact(
		ctx,
		a.s3c,
		"omegaup-backup",
		path.Join("omegaup", backupSubmissionKey),
		path.Join(ctx.Config.Grader.V1.RuntimePath, backupSubmissionKey),
		bytes.NewReader(buf.Bytes()),
	)
	if err != nil {
		return fmt.Errorf("put omegaup-backup: %w", err)
	}

	// TODO: leave just this version once the migration is done.
	return putArtifact(
		ctx,
		a.s3c,
		"omegaup-submissions",
		guid,
		path.Join(ctx.Config.Grader.V1.RuntimePath, backupSubmissionKey),
		bytes.NewReader(buf.Bytes()),
	)
}

// Artifacts is an interface to interact with grader artifacts.
type Artifacts interface {
	// Get returns a io.ReadCloser with the contents of the artifact.
	Get(ctx *common.Context, filename string) (io.ReadCloser, error)
	// Put atomically creates an artifact with the contents as specified by the
	// reader.
	Put(ctx *common.Context, filename string, r io.Reader) error
	// Clean cleans the local filesystem's contents. No attempt to clean the S3
	// artifacts is done.
	Clean() error
}

type graderArtifacts struct {
	s3c          *s3.S3
	gradeDir     string
	bucketPrefix string
}

func (a *graderArtifacts) Get(ctx *common.Context, filename string) (io.ReadCloser, error) {
	return getArtifact(
		ctx,
		a.s3c,
		"omegaup-runs",
		path.Join(a.bucketPrefix, filename),
		path.Join(a.gradeDir, filename),
	)
}

func (a *graderArtifacts) Put(ctx *common.Context, filename string, r io.Reader) error {
	return putArtifact(
		ctx,
		a.s3c,
		"omegaup-runs",
		path.Join(a.bucketPrefix, filename),
		path.Join(a.gradeDir, filename),
		r,
	)
}

func (a *graderArtifacts) Clean() error {
	return os.RemoveAll(a.gradeDir)
}

type localGraderArtifacts struct {
	gradeDir string
	token    string
}

func (a *localGraderArtifacts) Get(ctx *common.Context, filename string) (io.ReadCloser, error) {
	return os.Open(path.Join(a.gradeDir, filename))
}

func (a *localGraderArtifacts) Put(ctx *common.Context, filename string, r io.Reader) error {
	f, err := newAtomicFile(path.Join(a.gradeDir, filename))
	if err != nil {
		return err
	}
	defer f.cleanup()

	_, err = io.Copy(f.f, r)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return f.commit(nil)
}

func (a *localGraderArtifacts) Clean() error {
	return os.RemoveAll(a.gradeDir)
}
