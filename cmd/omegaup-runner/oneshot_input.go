package main

import (
	"encoding/json"
	"os"
	"path"

	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
)

const (
	oneshotInputHash = "0000000000000000000000000000000000000000"
)

// oneshotInputFactory is an InputFactory that produces a oneshotInput.
type oneshotInputFactory struct {
	problemPath string
}

var _ common.InputFactory = (*oneshotInputFactory)(nil)

func newOneshotInputFactory(problemPath string) *oneshotInputFactory {
	return &oneshotInputFactory{
		problemPath: problemPath,
	}
}

func (f *oneshotInputFactory) NewInput(
	hash string,
	mgr *common.InputManager,
) common.Input {
	return &oneshotInput{
		problemPath: f.problemPath,
	}
}

// oneshotInput is a pre-committed, read-only Input that can be used in oneshot
// mode. This expects the input to be a git checkout of a problem, which should
// contain a settings.json file.
type oneshotInput struct {
	problemPath string
	settings    common.ProblemSettings
	committed   bool
}

var _ common.Input = (*oneshotInput)(nil)

func (i *oneshotInput) Committed() bool {
	return i.committed
}

func (i *oneshotInput) Size() base.Byte {
	return base.Byte(0)
}

func (i *oneshotInput) Hash() string {
	return oneshotInputHash
}

func (i *oneshotInput) Path() string {
	return i.problemPath
}

func (i *oneshotInput) Settings() *common.ProblemSettings {
	return &i.settings
}

func (i *oneshotInput) Persist() error {
	settingsFd, err := os.Open(path.Join(i.problemPath, "settings.json"))
	if err != nil {
		return err
	}
	defer settingsFd.Close()
	decoder := json.NewDecoder(settingsFd)
	if err := decoder.Decode(i.Settings()); err != nil {
		return err
	}

	i.committed = true
	return nil
}

func (i *oneshotInput) Verify() error {
	// Always fail verification since we want any errors reading settings.json to
	// be fatal. That is achieved by parsing and storing it on Persist().
	return common.ErrUnimplemented
}

func (i *oneshotInput) Delete() error {
	return common.ErrUnimplemented
}

func (i *oneshotInput) Release() {
}
