package consul

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
)

// Register is the helper function to register service with Consul server
// name - service name
// host - service host
// port - service port
// target - consul dial address, for example: "127.0.0.1:8500"
// ttl - ttl of the register information
func Register(name string, host string, port int, target string, ttl int) error {
	conf := &api.Config{Scheme: "http", Address: target}
	client, err := api.NewClient(conf)
	if err != nil {
		return fmt.Errorf("Create consul client error: %v", err)
	}

	// service definition
	serviceID := fmt.Sprintf("%s-%s-%d", name, host, port)
	reg := &api.AgentServiceRegistration{
        ID: serviceID,
        Name: name,
        Port: port,
        Address: host,
		Check: &api.AgentServiceCheck{
			TTL: fmt.Sprintf("%ds", ttl),
			Status: api.HealthPassing,
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	// register service
	err = client.Agent().ServiceRegister(reg);
    if err != nil {
        return fmt.Errorf("Consul service register error: %v", err)
    }

	// ttl ticker
	ticker := time.NewTicker(time.Duration(ttl) * time.Second / 5)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT)

	for {
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
			ticker.Stop()
			client.Agent().ServiceDeregister(serviceID)
		case <-ticker.C:
			err := client.Agent().UpdateTTL(serviceID, "", "passing")
			if err != nil {
				grpclog.Infof("Consul registry updateTTL for %s failed: %v", name, err)
			}
		}
	}
}