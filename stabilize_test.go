package chord

import (
	"testing"
	"time"
)

func TestAdaptiveStabilize(t *testing.T) {
	stab := newAdaptiveStabilize(2*time.Second, 3*time.Second, 30*time.Second, 20)

	stab.rand()
	if stab.min != (2 * time.Second) {
		t.Fatal("wrong stab min", stab.min)
	}
	for i := 0; i < 20; i++ {
		stab.rand()
	}
	if stab.min != (3 * time.Second) {
		t.Fatal("wrong stab min", stab.min)
	}
}

func TestAdaptiveStabilize_collision(t *testing.T) {
	//stab := newAdaptiveStabilize(2*time.Second, 3*time.Second, 30*time.Second)
	stab := &adaptiveStabilize{min: 10 * time.Second, max: 20 * time.Second, threshold: 9 * time.Second}

	var times []time.Duration
	for i := 0; i < 1000; i++ {
		after := stab.rand()
		times = append(times, after)
		if after < stab.min {
			t.Fatalf("after below min")
		}
		if after > stab.max {
			t.Fatalf("after above max")
		}
	}

	collisions := 0
	for idx, val := range times {
		for i := 0; i < len(times); i++ {
			if idx != i && times[i] == val {
				collisions++
			}
		}
	}

	if collisions > 3 {
		t.Fatalf("too many collisions! %d", collisions)
	}
}
