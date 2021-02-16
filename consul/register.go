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
// context
// name - service name
// host - service host
// port - service port
// target - consul dial address, for example: "127.0.0.1:8500"
// ttl - ttl of the register information
func Register(ctx context.Context, name string, host string, port int, target string, ttl int) error {
	if ctx == nil {
		panic("nil context")
	}

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
			CheckID: serviceID,
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
	subCtx, cancel := context.WithCancel(ctx)

	// notify on signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT)

	go func() {
		for {
			select {
			case <-ch:
				cancel()
			case <-subCtx.Done():
				ticker.Stop()
				client.Agent().ServiceDeregister(serviceID)
				return
			case <-ticker.C:
				err := client.Agent().UpdateTTL(serviceID, "TTL active", api.HealthPassing)
				if err != nil {
					grpclog.Infof("Consul registry updateTTL for %s failed: %v", name, err)
				}
			}
		}
	}()

	return nil
}