package consul

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

type consulResolver struct {
	cc           resolver.ClientConn
	consulHealth consulHealthEndpoint
	service      string
	tags         []string
	ctx          context.Context
	cancel       context.CancelFunc
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
		addr := e.Service.Address
		if addr == "" {
			addr = e.Node.Address

			if grpclog.V(2) {
				grpclog.Infof("grpc: consul service '%s' has no ServiceAddress, using agent address '%+v'", e.Service.ID, addr)
			}
		}

		result = append(result, resolver.Address{
			Addr:       fmt.Sprintf("%s:%d", addr, e.Service.Port),
			ServerName: e.Service.ID,
		})
	}

	if grpclog.V(1) {
		grpclog.Infof("grpc: consul service '%s' resolved to '%+v'", c.service, result)
	}

	return result, meta.LastIndex, nil
}

func (c *consulResolver) watcher() {
	var lastAddrs []resolver.Address

	opts := (&consul.QueryOptions{}).WithContext(c.ctx)

	defer c.wgStop.Done()

	for {
		for {
			var addrs []resolver.Address
			var err error

			addrs, opts.WaitIndex, err = c.query(opts)
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
			if (len(addrs) == 0 && len(lastAddrs) == 0) ||
				reflect.DeepEqual(addrs, lastAddrs) {
				// sleep a bit to prevent that query() is
				// called in a tight-loop if the consul server
				// returns multiple time immediately, despite
				// that we pass a waitindex and ask for a
				// blocking query. This should not happen,
				// except the consul server is buggy.
				time.Sleep(50 * time.Millisecond)
				continue
			}

			c.cc.UpdateState(resolver.State{Addresses: addrs})
			lastAddrs = addrs
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
