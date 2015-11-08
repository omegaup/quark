package runner

import (
	"github.com/omegaup/quark/common"
)

func CalculateScore(ctx *common.Context,
	settings *common.ProblemSettings, caseData common.CaseSettings,
	expectedOutput, contestantOutput string) float64 {
	return 0.5
}
