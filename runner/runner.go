package runner

import (
	"fmt"
	"github.com/omegaup/quark/common"
)

func Grade(run *common.Run, input common.Input) {
	fmt.Printf("Running %v with %v\n", run, input)
}
