package consul

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

type healthFilter int

const (
	healthFilterUndefined healthFilter = iota
	healthFilterOnlyHealthy
	healthFilterFallbackToUnhealthy
)

type consulResolver struct {
	cc           resolver.ClientConn
	consulHealth consulHealthEndpoint
	service      string
	tags         []string
	healthFilter healthFilter
	ctx          context.Context
	cancel       context.CancelFunc
	resolveNow   chan struct{}
	wgStop       sync.WaitGroup
}

type consulHealthEndpoint interface {
	ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

// consulCreateHealthClientFn can be overwritten in tests to make
// newConsulResolver() return a different consulHealthEndpoint implementation
var consulCreateHealthClientFn = func(cfg *consul.Config) (consulHealthEndpoint, error) {
	clt, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return clt.Health(), nil
}

func newConsulResolver(
	cc resolver.ClientConn,
	scheme, consulAddr, consulService string,
	tags []string,
	healthFilter healthFilter,
) (*consulResolver, error) {
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
		healthFilter: healthFilter,
		ctx:          ctx,
		cancel:       cancel,
		resolveNow:   make(chan struct{}, 1),
	}, nil
}

func (c *consulResolver) start() {
	c.wgStop.Add(1)
	go c.watcher()
}

func (c *consulResolver) query(opts *consul.QueryOptions) ([]resolver.Address, uint64, error) {
	entries, meta, err := c.consulHealth.ServiceMultipleTags(c.service, c.tags, c.healthFilter == healthFilterOnlyHealthy, opts)
	if err != nil {
		grpclog.Infof("grpcconsulresolver: resolving service name '%s' via consul failed: %v\n",
			c.service, err)

		return nil, 0, err
	}

	if c.healthFilter == healthFilterFallbackToUnhealthy {
		entries = filterPreferOnlyHealthy(entries)
	}

	result := make([]resolver.Address, 0, len(entries))
	for _, e := range entries {
		addr := e.Service.Address
		if addr == "" {
			addr = e.Node.Address

			if grpclog.V(2) {
				grpclog.Infof("grpcconsulresolver: service '%s' has no ServiceAddress, using agent address '%+v'", e.Service.ID, addr)
			}
		}

		result = append(result, resolver.Address{
			Addr: fmt.Sprintf("%s:%d", addr, e.Service.Port),
		})
	}

	sortAddresses(result)

	if grpclog.V(1) {
		grpclog.Infof("grpcconsulresolver: service '%s' resolved to '%+v'", c.service, result)
	}

	return result, meta.LastIndex, nil
}

// filterPreferOnlyHealthy if entries contains services with passing health
// check only entries with passing health are returned.
// Otherwise entries is returned unchanged.
func filterPreferOnlyHealthy(entries []*consul.ServiceEntry) []*consul.ServiceEntry {
	healthy := make([]*consul.ServiceEntry, 0, len(entries))

	for _, e := range entries {
		if e.Checks.AggregatedStatus() == api.HealthPassing {
			healthy = append(healthy, e)
		}
	}

	if len(healthy) != 0 {
		return healthy
	}

	return entries
}

func sortAddresses(addrs []resolver.Address) {
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].Addr < addrs[j].Addr
	})
}

func (c *consulResolver) watcher() {
	var lastReportedAddrs []resolver.Address
	var lastWaitIndex uint64

	opts := (&consul.QueryOptions{}).WithContext(c.ctx)

	defer c.wgStop.Done()

	for {
		for {
			var addrs []resolver.Address
			var err error

			queryStartTime := time.Now()
			addrs, opts.WaitIndex, err = c.query(opts)
			lastWaitIndex = opts.WaitIndex
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				// After ReportError() was called, the grpc
				// loadbalancer will call ResolveNow()
				// periodically to retry. Therefore we do not
				// have to retry on our own by e.g.  setting
				// the timer.
				c.cc.ReportError(err)
				break
			}

			// query() blocks until a consul internal timeout expired or
			// data newer then the passed opts.WaitIndex is available.
			// We check if the returned addrs changed to not call
			// cc.UpdateState() unnecessary for unchanged addresses.
			if (len(addrs) == 0 && len(lastReportedAddrs) == 0) ||
				reflect.DeepEqual(addrs, lastReportedAddrs) {

				// If the consul server responds with
				// the same data then in the last
				// query in less then 50ms, we sleep a
				// bit to prevent querying in a tight loop
				// This should only happen if the consul server
				// is buggy but better be safe. :-)
				if lastWaitIndex == opts.WaitIndex &&
					time.Since(queryStartTime) < 50*time.Millisecond {
					grpclog.Warningf("grpcconsulresolver: consul responded too fast with same data and waitIndex (%d) then in previous query, delaying next query",
						opts.WaitIndex)
					time.Sleep(50 * time.Millisecond)
				}

				continue
			}

			c.cc.UpdateState(resolver.State{Addresses: addrs})
			lastReportedAddrs = addrs
		}

		select {
		case <-c.ctx.Done():
			return

		case <-c.resolveNow:
		}
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
}
