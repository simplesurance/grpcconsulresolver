package mocks

import (
	"sync"

	consul "github.com/hashicorp/consul/api"
)

type ConsulHealthClient struct {
	mutex     sync.Mutex
	entries   []*consul.ServiceEntry
	queryMeta consul.QueryMeta
	err       error
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.entries = entries
}

func (c *ConsulHealthClient) SetRespError(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.err = err
}

func (c *ConsulHealthClient) ServiceMultipleTags(_ string, _ []string, _ bool, q *consul.QueryOptions) ([]*consul.ServiceEntry, *consul.QueryMeta, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if q.Context().Err() != nil {
		return nil, nil, q.Context().Err()
	}

	return c.entries, &c.queryMeta, c.err
}
