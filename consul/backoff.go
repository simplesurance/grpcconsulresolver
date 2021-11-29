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

var defBackoff = backoff{
	intervals: []time.Duration{
		500 * time.Millisecond,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
	},

	jitterPct: 20,
	jitterSrc: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (b *backoff) Backoff(retry int) time.Duration {
	idx := retry

	if idx < 0 || idx > len(b.intervals)-1 {
		idx = len(b.intervals) - 1
	}

	d := b.intervals[idx]

	return d + time.Duration(((int64(d) / 100) * int64((b.jitterSrc.Intn(b.jitterPct)))))
}
