package mocks

import (
	"sync"

	consul "github.com/hashicorp/consul/api"
)

type ConsulHealthClient struct {
	Mutex                 sync.Mutex
	Entries               []*consul.ServiceEntry
	queryMeta             consul.QueryMeta
	ResolveCnt            int
	Err                   error
	ServiceMultipleTagsFn func(*ConsulHealthClient, string, []string, bool, *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error)
}

func NewConsulHealthClient() *ConsulHealthClient {
	return &ConsulHealthClient{}
}

func (c *ConsulHealthClient) SetRespServiceEntries(s []*consul.AgentService) {
	serviceEntries := []*consul.ServiceEntry{}
	for _, e := range s {
		serviceEntries = append(serviceEntries,
			&consul.ServiceEntry{
				Service: e,
			})
	}

	c.SetRespEntries(serviceEntries)
}

func (c *ConsulHealthClient) SetRespEntries(entries []*consul.ServiceEntry) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.Entries = entries
}

func (c *ConsulHealthClient) SetRespError(err error) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.Err = err
}

func (c *ConsulHealthClient) ServiceMultipleTags(_ string, _ []string, _ bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	if c.ServiceMultipleTagsFn != nil {
		return c.ServiceMultipleTagsFn(c, "", nil, false, q)
	}

	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.ResolveCnt++

	if q.Context().Err() != nil {
		return nil, nil, q.Context().Err()
	}

	return c.Entries, &c.queryMeta, c.Err
}

func (c *ConsulHealthClient) ResolveCount() int {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.ResolveCnt
}
