package consul

import (
	"math/rand"
	"time"
)

type backoff struct {
	intervals []time.Duration
	jitterPct int
	jitterSrc *rand.Rand
}

func defaultBackoff() *backoff {
	return &backoff{
		intervals: []time.Duration{
			10 * time.Millisecond,
			50 * time.Millisecond,
			250 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			2 * time.Second,
			3 * time.Second,
			4 * time.Second,
			5 * time.Second,
		},

		jitterPct: 10,
		jitterSrc: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *backoff) Backoff(retry int) time.Duration {
	idx := retry

	if idx < 0 || idx > len(b.intervals)-1 {
		idx = len(b.intervals) - 1
	}

	d := b.intervals[idx]
	jitter := time.Duration((int64(d) / 100) * int64(b.jitterSrc.Intn(b.jitterPct)))

	if b.jitterSrc.Intn(2) == 0 {
		return d + jitter
	}

	return d - jitter
}
