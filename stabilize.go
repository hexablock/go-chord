package chord

import (
	"math/rand"
	"time"
)

type adaptiveStabilize struct {
	min time.Duration // current stabilize min
	max time.Duration // current stabilize max
	c   int           // current number for stay count

	startMin time.Duration // starting min value
	startMax time.Duration // starting max value

	threshold time.Duration // threshold to stop stepping up
	stayCount int           // number of iterations before we start stepping
}

func newAdaptiveStabilize(startMin, startMax, threshold time.Duration, stayCount int) *adaptiveStabilize {
	as := &adaptiveStabilize{
		startMin:  startMin,
		startMax:  startMax,
		threshold: threshold,
		stayCount: stayCount,
	}
	as.reset()
	return as
}

// double on each call
func (stab *adaptiveStabilize) step() {
	stab.min += (stab.min / 2)
	stab.max += (stab.max / 2)
}

func (stab *adaptiveStabilize) reset() {
	stab.min = stab.startMin
	stab.max = stab.startMax
	stab.c = 0
}

// return random time duration between min and max
func (stab *adaptiveStabilize) rand() time.Duration {
	r := rand.Float64()
	d := time.Duration((r * float64(stab.max-stab.min)) + float64(stab.min))

	// check if adaptive stabilize is enabled
	if stab.min < stab.threshold {
		if stab.c > stab.stayCount {
			stab.step()
		} else {
			stab.c++
		}
	}

	return d
}
