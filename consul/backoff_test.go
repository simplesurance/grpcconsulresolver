package consul

import (
	"testing"
	"time"
)

func maxBackoffJitter(idx int) time.Duration {
	return time.Duration(int64(defBackoff.intervals[idx]) / 100 * int64(defBackoff.jitterPct))
}

func TestBackoff(t *testing.T) {
	for i := range defBackoff.intervals {
		d := defBackoff.Backoff(i)
		if d < defBackoff.intervals[i] {
			t.Errorf("duration for retry %d is %d expected >= %d", i, d, defBackoff.intervals[i])
		}

		maxJitter := maxBackoffJitter(i)
		if d >= d+maxJitter {
			t.Errorf("duration for retry %d is %d expected <= %d", i, d, d+maxJitter)
		}
	}

}

func TestBackoff_IntervalIdxBounds(t *testing.T) {
	tests := []struct {
		name  string
		retry int
		idx   int
	}{
		{
			name:  "retry: -1",
			retry: -1,
			idx:   0,
		},
		{
			name:  "retry > interval len",
			retry: len(defBackoff.intervals),
			idx:   len(defBackoff.intervals) - 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retry := tt.retry

			d := defBackoff.Backoff(retry)
			if d < defBackoff.intervals[tt.idx] {
				t.Errorf("duration for retry %d is %d expected >= %d", retry, d, defBackoff.intervals[tt.idx])
			}

			maxJitter := maxBackoffJitter(tt.idx)
			if d >= d+maxJitter {
				t.Errorf("duration for retry %d is %d expected <= %d", retry, d, d+maxJitter)
			}

		})
	}
}
