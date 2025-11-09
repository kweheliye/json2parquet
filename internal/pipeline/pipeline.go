package pipeline

import (
	"github.com/kweheliye/json2parquet/utils"
)

// Step represents a unit of work in a pipeline
// Each step should perform its task and exit the process with log.Fatalf on unrecoverable errors.
type Step interface {
	Name() string
	Run()
}

// Pipeline executes a sequence of steps in order
type Pipeline struct {
	steps []Step
}

// New constructs a Pipeline from given steps
func New(steps ...Step) *Pipeline {
	return &Pipeline{steps: steps}
}

// Run executes all steps sequentially
func (p *Pipeline) Run() {
	for i, step := range p.steps {
		log.Infof("[Pipeline] Step %d/%d: %step - starting", i+1, len(p.steps), step.Name())

		elapsed := utils.Timed(func() {
			step.Run()
		})
		log.Infof("[Pipeline] Step %d/%d: %step - done in %.3f step", i+1, len(p.steps), step.Name(), elapsed.Seconds())
	}
}
