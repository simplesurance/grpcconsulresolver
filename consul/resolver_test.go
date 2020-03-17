package consul

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type testClientConn struct {
	mutex             sync.Mutex
	addrs             []resolver.Address
	newAddressCallCnt int
	lastReportedError error
}

func (t *testClientConn) ParseServiceConfig(_ string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{Err: errors.New("config parsing not implemented in test mock")}
}

func (t *testClientConn) ReportError(err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.lastReportedError = err
}

func (t *testClientConn) UpdateState(state resolver.State) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.addrs = state.Addresses
	t.newAddressCallCnt++
}

func (t *testClientConn) NewAddress(addrs []resolver.Address) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.addrs = addrs
	t.newAddressCallCnt++
}

func (t *testClientConn) getNewAddressCallCnt() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.newAddressCallCnt
}

func (t *testClientConn) getAddrs() (addrs []resolver.Address) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.addrs
}

func (*testClientConn) NewServiceConfig(string) {
}

type consulMockHealthClient struct {
	mutex     sync.Mutex
	entries   []*consul.ServiceEntry
	queryMeta consul.QueryMeta
	err       error
}

func (c *consulMockHealthClient) setResolveAddrs(s []*consul.AgentService) {
	serviceEntries := []*consul.ServiceEntry{}
	for _, e := range s {
		serviceEntries = append(serviceEntries,
			&consul.ServiceEntry{
				Service: e,
			})
	}

	c.setResolveAddrsEntries(serviceEntries)
}

func (c *consulMockHealthClient) setResolveAddrsEntries(entries []*consul.ServiceEntry) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.entries = entries
}

func (c *consulMockHealthClient) ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if q.Context().Err() != nil {
		return nil, nil, q.Context().Err()
	}

	return c.entries, &c.queryMeta, c.err
}

func replaceCreateHealthClientFn(fn func(cfg *consul.Config) (consulHealthEndpoint, error)) func() {
	old := consulCreateHealthClientFn

	consulCreateHealthClientFn = fn

	return func() {
		consulCreateHealthClientFn = old
	}
}

func asHasEntry(agentServices []*consul.AgentService, addr *resolver.Address) bool {
	for _, as := range agentServices {
		if fmt.Sprintf("%s:%d", as.Address, as.Port) == addr.Addr {
			return true
		}
	}

	return false
}

func resolvAddrsHasEntry(addrs []resolver.Address, as *consul.AgentService) bool {
	for _, addr := range addrs {
		if fmt.Sprintf("%s:%d", as.Address, as.Port) == addr.Addr {
			return true
		}
	}

	return false
}

func cmpResolveResults(addrs []resolver.Address, ags []*consul.AgentService) bool {
	for _, addr := range addrs {
		if !asHasEntry(ags, &addr) {
			return false
		}
	}
	for _, as := range ags {
		if !resolvAddrsHasEntry(addrs, as) {
			return false
		}
	}

	return true
}

func resolverAddressExist(addrs []resolver.Address, wanted resolver.Address) bool {
	for _, addr := range addrs {
		if addr == wanted {
			return true
		}
	}

	return false
}

func cmpAddrs(addrsA, addrsB []resolver.Address) bool {
	for _, a := range addrsA {
		if !resolverAddressExist(addrsB, a) {
			return false
		}
	}

	for _, b := range addrsB {
		if !resolverAddressExist(addrsA, b) {
			return false
		}
	}

	return true
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name           string
		target         resolver.Target
		consulResponse []*consul.ServiceEntry
		resolverResult []resolver.Address
	}{
		{
			name:   "defaultResolve",
			target: resolver.Target{Endpoint: "user-service"},
			consulResponse: []*consul.ServiceEntry{
				{
					Service: &api.AgentService{
						Address: "localhost",
						Port:    5678,
					},
				},
				{
					Service: &api.AgentService{
						Address: "remotehost",
						Port:    1234,
					},
				},
			},
			resolverResult: []resolver.Address{
				{
					Addr: "localhost:5678",
				},
				{
					Addr: "remotehost:1234",
				},
			},
		},

		{
			name:   "fallbackToUnhealthy_ResolveToOnlyHealthy",
			target: resolver.Target{Endpoint: "credit-service?health=fallbackToUnhealthy"},
			consulResponse: []*consul.ServiceEntry{
				{
					Service: &api.AgentService{
						Address: "localhost",
						Port:    5678,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthPassing,
						},
					},
				},
				{
					Service: &api.AgentService{
						Address: "remotehost",
						Port:    9,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthPassing,
						},
					},
				},
				{
					Service: &api.AgentService{
						Address: "unhealthyHost",
						Port:    1234,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthCritical,
						},
					},
				},
				{
					Service: &api.AgentService{
						Address: "warnedHost",
						Port:    1,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthWarning,
						},
					},
				},
			},
			resolverResult: []resolver.Address{
				{
					Addr: "localhost:5678",
				},

				{
					Addr: "remotehost:9",
				},
			},
		},

		{
			name:   "fallbackToUnhealthy_AllUnhealthy",
			target: resolver.Target{Endpoint: "web-service?health=fallbackToUnhealthy"},
			consulResponse: []*consul.ServiceEntry{
				{
					Service: &api.AgentService{
						Address: "localhost",
						Port:    5678,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthCritical,
						},
					},
				},
				{
					Service: &api.AgentService{
						Address: "remotehost",
						Port:    1234,
					},
					Checks: api.HealthChecks{
						{
							Status: api.HealthCritical,
						},
					},
				},
			},
			resolverResult: []resolver.Address{
				{
					Addr: "localhost:5678",
				},
				{
					Addr: "remotehost:1234",
				},
			},
		},
	}

	health := &consulMockHealthClient{}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.target.Endpoint, func(t *testing.T) {
			cc := testClientConn{}
			newAddressCallCnt := cc.getNewAddressCallCnt()
			b := NewBuilder()

			health.setResolveAddrsEntries(tt.consulResponse)

			r, err := b.Build(tt.target, &cc, resolver.BuildOptions{})
			if err != nil {
				t.Fatal("Build() failed:", err.Error())
			}

			r.ResolveNow(resolver.ResolveNowOptions{})

			var addrs []resolver.Address
			for newAddressCallCnt == cc.getNewAddressCallCnt() {
				time.Sleep(time.Millisecond)
			}

			addrs = cc.getAddrs()
			if !cmpAddrs(addrs, tt.resolverResult) {
				t.Errorf("resolved address '%+v', expected: '%+v'", addrs, tt.resolverResult)
			}

			r.Close()
		})
	}
}

func TestResolveNewAddressOnlyCalledOnChange(t *testing.T) {
	health := &consulMockHealthClient{}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	addr := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},
	}

	cc := testClientConn{}
	newAddressCallCnt := cc.getNewAddressCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()

	health.setResolveAddrs(addr)

	r, err := b.Build(target, &cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}
	newAddressCallCnt = cc.getNewAddressCallCnt()

	r.ResolveNow(resolver.ResolveNowOptions{})
	time.Sleep(time.Second)

	if newAddressCallCnt != cc.getNewAddressCallCnt() {
		t.Error("cc.NewAddress() was called despite resolved addresses did not change")
	}

	r.Close()
}

func TestResolveAddrChange(t *testing.T) {
	health := &consulMockHealthClient{}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	addrs1 := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},
	}

	addrs2 := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},

		&consul.AgentService{
			Address: "remotehost",
			Port:    12345,
		},
	}

	cc := testClientConn{}
	newAddressCallCnt := cc.getNewAddressCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()

	health.setResolveAddrs(addrs1)

	r, err := b.Build(target, &cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs := cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs1) {
		t.Errorf("resolved address '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	newAddressCallCnt = cc.getNewAddressCallCnt()
	health.setResolveAddrs(addrs2)
	r.ResolveNow(resolver.ResolveNowOptions{})
	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs = cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs2) {
		t.Errorf("resolved address after change '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	r.Close()
}

func TestResolveAddrChangesToUnresolvable(t *testing.T) {
	health := &consulMockHealthClient{}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	addrs1 := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},
	}
	addrs2 := []*consul.AgentService{}

	cc := testClientConn{}
	newAddressCallCnt := cc.getNewAddressCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()

	health.setResolveAddrs(addrs1)

	r, err := b.Build(target, &cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs := cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs1) {
		t.Errorf("resolved address '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	newAddressCallCnt = cc.getNewAddressCallCnt()
	health.setResolveAddrs(addrs2)
	r.ResolveNow(resolver.ResolveNowOptions{})
	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs = cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs2) {
		t.Errorf("resolved address after change '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	r.Close()
}

func TestErrorIsReportedOnQueryErrors(t *testing.T) {
	queryErr := errors.New("query failed")
	health := &consulMockHealthClient{err: queryErr}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	cc := testClientConn{}
	b := NewBuilder()
	target := resolver.Target{Endpoint: "user-service"}

	r, err := b.Build(target, &cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	gethealthErr := func() error {
		cc.mutex.Lock()
		defer cc.mutex.Unlock()

		return cc.lastReportedError
	}

	for ; err == nil; err = gethealthErr() {
		time.Sleep(time.Millisecond)

	}

	if err != queryErr {
		t.Fatalf("resolver error is %+v, expected %+v", err, queryErr)
	}
}
