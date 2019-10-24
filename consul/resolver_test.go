package consul

import (
	"fmt"
	"sync"
	"testing"
	"time"

	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/resolver"
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

type testClientConn struct {
	mutex             sync.Mutex
	addrs             []resolver.Address
	newAddressCallCnt int
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.entries = []*consul.ServiceEntry{}

	for _, e := range s {
		c.entries = append(c.entries,
			&consul.ServiceEntry{
				Service: e,
			})
	}
}

func (c *consulMockHealthClient) ServiceMultipleTags(service string, tags []string, passingOnly bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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

func TestResolve(t *testing.T) {
	health := &consulMockHealthClient{}
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	tests := []struct {
		target resolver.Target
		result []*consul.AgentService
	}{
		{
			resolver.Target{Endpoint: "user-service"},
			[]*consul.AgentService{
				&consul.AgentService{
					Address: "localhost",
					Port:    5678,
				},
			},
		},
	}

	for _, tt := range tests {
		cc := testClientConn{}
		newAddressCallCnt := cc.getNewAddressCallCnt()
		b := NewBuilder()

		health.setResolveAddrs(tt.result)

		r, err := b.Build(tt.target, &cc, resolver.BuildOption{})
		if err != nil {
			t.Fatal("Build() failed:", err.Error())
		}

		r.ResolveNow(resolver.ResolveNowOption{})

		var addrs []resolver.Address
		for newAddressCallCnt == cc.getNewAddressCallCnt() {
			time.Sleep(time.Millisecond)
		}

		addrs = cc.getAddrs()
		if !cmpResolveResults(addrs, tt.result) {
			t.Errorf("resolved address '%+v', expected: '%+v'", addrs, tt.result)
		}

		r.Close()
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

	r, err := b.Build(target, &cc, resolver.BuildOption{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOption{})

	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}
	newAddressCallCnt = cc.getNewAddressCallCnt()

	r.ResolveNow(resolver.ResolveNowOption{})
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

	r, err := b.Build(target, &cc, resolver.BuildOption{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOption{})

	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs := cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs1) {
		t.Errorf("resolved address '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	newAddressCallCnt = cc.getNewAddressCallCnt()
	health.setResolveAddrs(addrs2)
	r.ResolveNow(resolver.ResolveNowOption{})
	for newAddressCallCnt == cc.getNewAddressCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs = cc.getAddrs()
	if !cmpResolveResults(resolvedAddrs, addrs2) {
		t.Errorf("resolved address after change '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	r.Close()
}
