package consul

import (
	"fmt"
	"testing"
	"time"
)

func minBackoff(t time.Duration, jitterPCT int) time.Duration {
	return t - t*time.Duration(jitterPCT)/100
}

func maxBackoff(t time.Duration, jitterPCT int) time.Duration {
	return t + t*time.Duration(jitterPCT)/100
}

func TestBackoff_IntervalIdxBounds(t *testing.T) {
	b := defaultBackoff()

	for i := range []int{-100000, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99999999} {
		t.Run(fmt.Sprintf("retry-%d", i), func(t *testing.T) {
			d := b.Backoff(-10)

			minB := minBackoff(b.intervals[len(b.intervals)-1], b.jitterPct)
			maxB := maxBackoff(b.intervals[len(b.intervals)-1], b.jitterPct)

			if d < minB {
				t.Errorf("backoff is %s, expecting >%s", d, minB)
			}

			if d > maxB {
				t.Errorf("backoff is %s, expecting <%s", d, minB)
			}
		})
	}
}
