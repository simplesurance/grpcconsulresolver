package consul

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	consul "github.com/hashicorp/consul/api"
	"github.com/simplesurance/grpcconsulresolver/mocks"
	"google.golang.org/grpc/resolver"
)

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
			name:   "UseAgentAddrIfServiceAddrEmpty",
			target: resolver.Target{Endpoint: "user-service"},
			consulResponse: []*consul.ServiceEntry{
				{
					Service: &consul.AgentService{
						Port: 5678,
					},
					Node: &consul.Node{
						Address: "localhost",
					},
				},
			},
			resolverResult: []resolver.Address{
				{
					Addr: "localhost:5678",
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

	health := mocks.NewConsulHealthClient()
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	for _, tt := range tests {
		t.Run(tt.target.Endpoint, func(t *testing.T) {
			cc := mocks.NewClientConn()
			newAddressCallCnt := cc.UpdateStateCallCnt()
			b := NewBuilder()

			health.SetRespEntries(tt.consulResponse)

			r, err := b.Build(tt.target, cc, resolver.BuildOptions{})
			if err != nil {
				t.Fatal("Build() failed:", err.Error())
			}

			r.ResolveNow(resolver.ResolveNowOptions{})

			var addrs []resolver.Address
			for newAddressCallCnt == cc.UpdateStateCallCnt() {
				time.Sleep(time.Millisecond)
			}

			addrs = cc.Addrs()
			if !cmpAddrs(addrs, tt.resolverResult) {
				t.Errorf("resolved address '%+v', expected: '%+v'", addrs, tt.resolverResult)
			}

			r.Close()
		})
	}
}

func TestResolveNewAddressOnlyCalledOnChange(t *testing.T) {
	health := mocks.NewConsulHealthClient()
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	service := []*consul.ServiceEntry{
		&consul.ServiceEntry{
			Service: &consul.AgentService{
				Address: "localhost",
				Port:    5678,
			},
		},
	}

	cc := mocks.NewClientConn()
	newAddressCallCnt := cc.UpdateStateCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()

	health.SetRespEntries(service)

	r, err := b.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.UpdateStateCallCnt() {
		time.Sleep(time.Millisecond)
	}
	newAddressCallCnt = cc.UpdateStateCallCnt()

	r.ResolveNow(resolver.ResolveNowOptions{})
	time.Sleep(time.Second)

	if newAddressCallCnt != cc.UpdateStateCallCnt() {
		t.Error("cc.NewAddress() was called despite resolved addresses did not change")
	}

	r.Close()
}

func TestResolveAddrChange(t *testing.T) {
	health := mocks.NewConsulHealthClient()
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	services1 := []*consul.ServiceEntry{
		&consul.ServiceEntry{
			Service: &consul.AgentService{
				Address: "localhost",
				Port:    5678,
			},
		},
	}

	addrs1 := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},
	}

	services2 := []*consul.ServiceEntry{
		&consul.ServiceEntry{
			Service: &consul.AgentService{
				Address: "localhost",
				Port:    5678,
			},
		},
		&consul.ServiceEntry{
			Service: &consul.AgentService{
				Address: "remotehost",
				Port:    12345,
			},
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

	cc := mocks.NewClientConn()
	newAddressCallCnt := cc.UpdateStateCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()
	health.SetRespEntries(services1)

	r, err := b.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.UpdateStateCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs := cc.Addrs()
	if !cmpResolveResults(resolvedAddrs, addrs1) {
		t.Errorf("resolved address '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	newAddressCallCnt = cc.UpdateStateCallCnt()
	health.SetRespEntries(services2)
	r.ResolveNow(resolver.ResolveNowOptions{})
	for newAddressCallCnt == cc.UpdateStateCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs = cc.Addrs()
	if !cmpResolveResults(resolvedAddrs, addrs2) {
		t.Errorf("resolved address after change '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	r.Close()
}

func TestResolveAddrChangesToUnresolvable(t *testing.T) {
	health := mocks.NewConsulHealthClient()
	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	services1 := []*consul.ServiceEntry{
		&consul.ServiceEntry{
			Service: &consul.AgentService{
				Address: "localhost",
				Port:    5678,
			},
		},
	}

	addrs1 := []*consul.AgentService{
		&consul.AgentService{
			Address: "localhost",
			Port:    5678,
		},
	}

	services2 := []*consul.ServiceEntry{}

	addrs2 := []*consul.AgentService{}

	cc := mocks.NewClientConn()
	newAddressCallCnt := cc.UpdateStateCallCnt()
	target := resolver.Target{Endpoint: "user-service"}
	b := NewBuilder()

	health.SetRespEntries(services1)

	r, err := b.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for newAddressCallCnt == cc.UpdateStateCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs := cc.Addrs()
	if !cmpResolveResults(resolvedAddrs, addrs1) {
		t.Errorf("resolved address '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	newAddressCallCnt = cc.UpdateStateCallCnt()
	health.SetRespEntries(services2)
	r.ResolveNow(resolver.ResolveNowOptions{})
	for newAddressCallCnt == cc.UpdateStateCallCnt() {
		time.Sleep(time.Millisecond)
	}

	resolvedAddrs = cc.Addrs()
	if !cmpResolveResults(resolvedAddrs, addrs2) {
		t.Errorf("resolved address after change '%+v', expected: '%+v'", resolvedAddrs, addrs1)
	}

	r.Close()
}

func TestErrorIsReportedOnQueryErrors(t *testing.T) {
	queryErr := errors.New("query failed")
	health := mocks.NewConsulHealthClient()
	health.SetRespError(queryErr)

	cleanup := replaceCreateHealthClientFn(
		func(cfg *consul.Config) (consulHealthEndpoint, error) {
			return health, nil
		},
	)
	defer cleanup()

	cc := mocks.NewClientConn()
	b := NewBuilder()
	target := resolver.Target{Endpoint: "user-service"}

	r, err := b.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal("Build() failed:", err.Error())
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	for ; err == nil; err = cc.LastReportedError() {
		time.Sleep(time.Millisecond)

	}

	if err != queryErr {
		t.Fatalf("resolver error is %+v, expected %+v", err, queryErr)
	}
}
