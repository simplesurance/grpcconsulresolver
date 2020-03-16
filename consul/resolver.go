package consul

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

type backoff struct {
	intervals []time.Duration
	jitterPct int
	jitterSrc *rand.Rand
}

var defBackoff = backoff{
	intervals: []time.Duration{
		time.Second,
		2 * time.Second,
		3 * time.Second,
		4 * time.Second,
		5 * time.Second,
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

type consulResolver struct {
	cc           resolver.ClientConn
	consulHealth consulHealthEndpoint
	service      string
	tags         []string
	ctx          context.Context
	cancel       context.CancelFunc
	timer        *time.Timer
	backoff      *backoff
	resolveNow   chan struct{}
	wgStop       sync.WaitGroup
}

type consulHealthEndpoint interface {
	ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

var consulCreateHealthClientFn = func(cfg *consul.Config) (consulHealthEndpoint, error) {
	clt, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return clt.Health(), nil
}

func newConsulResolver(cc resolver.ClientConn, scheme, consulAddr, consulService string, tags []string) (*consulResolver, error) {
	cfg := consul.Config{
		Address: consulAddr,
		Scheme:  scheme,
	}

	health, err := consulCreateHealthClientFn(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consul client failed. %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &consulResolver{
		cc:           cc,
		consulHealth: health,
		service:      consulService,
		tags:         tags,
		ctx:          ctx,
		cancel:       cancel,
		timer:        time.NewTimer(0),
		backoff:      &defBackoff,
		resolveNow:   make(chan struct{}, 1),
	}, nil
}

func (c *consulResolver) start() {
	c.wgStop.Add(1)
	go c.watcher()
}

func (c *consulResolver) query(opts *consul.QueryOptions) ([]resolver.Address, uint64, error) {
	entries, meta, err := c.consulHealth.ServiceMultipleTags(c.service, c.tags, true, opts)
	if err != nil {
		grpclog.Infof("grpc: resolving service name '%s' via consul failed: %v\n",
			c.service, err)

		return nil, 0, err
	}

	result := make([]resolver.Address, 0, len(entries))
	for _, e := range entries {
		result = append(result, resolver.Address{
			Addr:       fmt.Sprintf("%s:%d", e.Service.Address, e.Service.Port),
			ServerName: e.Service.ID,
		})
	}

	if grpclog.V(1) {
		grpclog.Infof("grpc: consul service '%s' resolved to '%+v'", c.service, result)
	}

	return result, meta.LastIndex, nil
}

func (c *consulResolver) watcher() {
	var retryCount int
	var addrs []resolver.Address
	var lastAddrs []resolver.Address
	opts := (&consul.QueryOptions{}).WithContext(c.ctx)

	defer c.wgStop.Done()

	for {
		var err error

		select {
		case <-c.ctx.Done():
			return

		case <-c.timer.C:

		case <-c.resolveNow:
		}

		lastAddrs = addrs

		addrs, opts.WaitIndex, err = c.query(opts)
		if err != nil {
			retryCount++
			c.timer.Reset(c.backoff.Backoff(retryCount))

			continue
		}

		// query() blocks until a consul internal timeout expired or
		// new data then newer then the passed opts.WaitIndex is available.
		// Check if the returned addrs changed to not call
		// cc.NewAddress() unnecessary for unchanged addresses
		if len(addrs) == 0 || reflect.DeepEqual(addrs, lastAddrs) {
			c.timer.Reset(50 * time.Millisecond)
			continue
		}

		retryCount = 0
		c.cc.UpdateState(resolver.State{Addresses: addrs})
		c.timer.Reset(0)
	}
}

func (c *consulResolver) ResolveNow(o resolver.ResolveNowOptions) {
	select {
	case c.resolveNow <- struct{}{}:

	default:
	}
}

func (c *consulResolver) Close() {
	c.cancel()
	c.wgStop.Wait()
	c.timer.Stop()
}
