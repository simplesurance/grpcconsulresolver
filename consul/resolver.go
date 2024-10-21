package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

type healthFilter int

type consulResolver struct {
	cc             resolver.ClientConn
	consulHealth   consulHealthEndpoint
	service        string
	tags           []string
	healthFilter   healthFilter
	backoffCounter *backoff
	ctx            context.Context
	cancel         context.CancelFunc
	resolveNow     chan struct{}
	wgStop         sync.WaitGroup

	lastReporterState state
}

type state struct {
	addresses []resolver.Address
	err       error
}

type consulHealthEndpoint interface {
	ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

const (
	healthFilterUndefined healthFilter = iota
	healthFilterOnlyHealthy
	healthFilterFallbackToUnhealthy
)

var logger = grpclog.Component("grpcconsulresolver")

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
	token string,
) (*consulResolver, error) {
	cfg := consul.Config{
		Address:  consulAddr,
		Scheme:   scheme,
		Token:    token,
		WaitTime: 10 * time.Minute,
	}

	health, err := consulCreateHealthClientFn(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consul client failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &consulResolver{
		cc:             cc,
		consulHealth:   health,
		service:        consulService,
		tags:           tags,
		healthFilter:   healthFilter,
		backoffCounter: defaultBackoff(),
		ctx:            ctx,
		cancel:         cancel,
		resolveNow:     make(chan struct{}, 1),
	}, nil
}

func (c *consulResolver) start() {
	c.wgStop.Add(1)
	go c.watcher()
}

func (c *consulResolver) query(opts *consul.QueryOptions) ([]resolver.Address, uint64, error) {
	if logger.V(2) {
		var tagsDescr, healthyDescr string
		if len(c.tags) > 0 {
			tagsDescr = "with tags: " + strings.Join(c.tags, ", ")
		}
		if c.healthFilter == healthFilterOnlyHealthy {
			healthyDescr = "healthy "
		}

		logger.Infof("querying consul for "+healthyDescr+"addresses of service '%s'"+tagsDescr, c.service)
	}

	entries, meta, err := c.consulHealth.ServiceMultipleTags(c.service, c.tags, c.healthFilter == healthFilterOnlyHealthy, opts)
	if err != nil {
		return nil, 0, err
	}

	if c.healthFilter == healthFilterFallbackToUnhealthy {
		entries = filterPreferOnlyHealthy(entries)
	}

	result := make([]resolver.Address, 0, len(entries))
	for _, e := range entries {
		// when additional fields are set in addr, addressesEqual()
		// must be updated to honor them
		addr := e.Service.Address
		if addr == "" {
			addr = e.Node.Address

			if logger.V(2) {
				logger.Infof("service '%s' has no ServiceAddress, using agent address '%+v'", e.Service.ID, addr)
			}
		}

		result = append(result, resolver.Address{
			Addr: net.JoinHostPort(addr, fmt.Sprint(e.Service.Port)),
		})
	}

	if logger.V(1) {
		logger.Infof("service '%s' resolved to '%+v'", c.service, result)
	}

	return slices.Clip(result), meta.LastIndex, nil
}

// filterPreferOnlyHealthy if entries contains services with passing health
// check only entries with passing health are returned.
// Otherwise entries is returned unchanged.
func filterPreferOnlyHealthy(entries []*consul.ServiceEntry) []*consul.ServiceEntry {
	healthy := make([]*consul.ServiceEntry, 0, len(entries))

	for _, e := range entries {
		if e.Checks.AggregatedStatus() == consul.HealthPassing {
			healthy = append(healthy, e)
		}
	}

	if len(healthy) != 0 {
		return healthy
	}

	return entries
}

func addressesEqual(a, b []resolver.Address) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}

	return slices.CompareFunc(a, b, func(e, e1 resolver.Address) int {
		return strings.Compare(e.Addr, e1.Addr)
	}) == 0
}

func (c *consulResolver) watcher() {
	var retryTimer *time.Timer
	var retryCnt int

	opts := (&consul.QueryOptions{}).WithContext(c.ctx)

	defer c.wgStop.Done()

	for {
		for {
			var addrs []resolver.Address
			var err error

			lastWaitIndex := opts.WaitIndex
			queryStartTime := time.Now()

			if retryTimer != nil {
				retryTimer.Stop()
			}

			// query() blocks until a consul internal timeout expired or
			// data newer then the passed opts.WaitIndex is available.
			addrs, opts.WaitIndex, err = c.query(opts)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				retryIn := c.backoffCounter.Backoff(retryCnt)
				logger.Infof("resolving service name '%s' via consul failed, retrying in %s: %s",
					c.service, retryIn, err)

				retryTimer = time.AfterFunc(c.backoffCounter.Backoff(retryCnt), func() {
					c.ResolveNow(resolver.ResolveNowOptions{})
				})
				retryCnt++

				c.reportError(err)
				break
			}
			retryCnt = 0

			if opts.WaitIndex < lastWaitIndex {
				logger.Infof("consul responded with a smaller waitIndex (%d) then the previous one (%d), restarting blocking query loop",
					opts.WaitIndex, lastWaitIndex)
				opts.WaitIndex = 0
				continue
			}

			if !c.reportAddress(addrs) {
				// If the consul server responds with
				// the same data than in the last
				// query in less than 50ms, sleep a
				// bit to prevent querying in a tight loop.
				// This should only happen if the consul server
				// is buggy but better be safe. :-)
				if lastWaitIndex == opts.WaitIndex &&
					time.Since(queryStartTime) < 50*time.Millisecond {
					logger.Warningf("consul responded too fast with same data and waitIndex (%d) than in previous query, delaying next query",
						opts.WaitIndex)
					time.Sleep(50 * time.Millisecond)
				}

				continue
			}
		}

		select {
		case <-c.ctx.Done():
			if retryTimer != nil {
				retryTimer.Stop()
			}

			return

		case <-c.resolveNow:
		}
	}
}

// reportAddress reports addrs to [c.cc.UpdateState] if it differs from the
// previous reported addresses or an error has been reported before.
// It returns true if [c.cc.UpdateState] has been called.
func (c *consulResolver) reportAddress(addrs []resolver.Address) bool {
	slices.SortFunc(addrs, func(e, e1 resolver.Address) int {
		return strings.Compare(e.Addr, e1.Addr)
	})

	if c.lastReporterState.err == nil && addressesEqual(addrs, c.lastReporterState.addresses) {
		return false
	}

	c.lastReporterState.addresses = addrs
	c.lastReporterState.err = nil

	err := c.cc.UpdateState(resolver.State{Addresses: addrs})
	if err != nil && logger.V(2) {
		// UpdateState errors can be ignored in
		// watch-based resolvers, see
		// https://github.com/grpc/grpc-go/issues/5048
		// for a detailed explanation.
		logger.Infof("ignoring error returned by UpdateState: %s", err)
	}

	return true
}

func (c *consulResolver) reportError(err error) bool {
	// We compare the string representation of the errors because it is
	// simple and works. http.Client.Do() returns [*url.Error]s which are not
	// equal when compared with "==", neither [url.Error.Err] does because
	// it e.g. can contain a *net.OpError.
	if c.lastReporterState.err != nil && c.lastReporterState.err.Error() == err.Error() {
		return false
	}

	c.lastReporterState.addresses = nil
	c.lastReporterState.err = err

	c.cc.ReportError(err)
	return true
}

func (c *consulResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case c.resolveNow <- struct{}{}:
	default:
	}
}

func (c *consulResolver) Close() {
	c.cancel()
	c.wgStop.Wait()
}
