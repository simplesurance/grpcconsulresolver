# Consul Resolver for grpc-go
![](https://github.com/simplesurance/grpcconsulresolver/workflows/ci/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/simplesurance/grpcconsulresolver)](https://goreportcard.com/report/github.com/simplesurance/grpcconsulresolver)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/github.com/simplesurance/grpcconsulresolver)

The repository provides a consul resolver for the
[GRPC-Go Package](https://github.com/grpc/grpc-go).

To register the resolver with the grpc-go run:

```go
resolver.Register(consul.NewBuilder())
```

Afterwards it can be used by calling grpc.Dial() and passing an URI in the
following format:

```
consul://[<consul-server>]/<serviceName>[?<OPT>[&<OPT>]...]
```

The default for `<consul-server>` is `127.0.0.1:8500`.

`<OPT>` is one of:

| OPT        | Format                          | Default  | Description                                                                                                                                                      |
|------------|---------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scheme     | `http\|https`                   | http     | Establish connection to consul via http or https.                                                                                                                |
| tags       | `<tag>,[,<tag>]...`             |          | Filter service by tags                                                                                                                                           |
| health     | `healthy\|fallbackToUnhealthy`  | healthy  | `healthy` resolves only to services with a passing health status.<br>`fallbackToUnhealthy` resolves to unhealthy ones if none exist with passing healthy status. |
| token      | `string`                        |          | Add X-Consul-Token header to requests in order to authenticate against ACL                                                                                       |

## Example

```go
package main

import (
  "google.golang.org/grpc"
  "google.golang.org/grpc/resolver"

  "github.com/simplesurance/grpcconsulresolver/consul"
)

func init() {
  // Register the consul consul at the grpc-go library
  resolver.Register(consul.NewBuilder())
}

func main() {
  // Create a GRPC-Client connection with the default load-balancer.
  // The addresses of the service "user-service" with the tags
  // "primary" and "eu" are resolved via the consul server "10.10.0.1:1234".
  // If no services with a passing health-checks are available, the connection
  // is established to unhealthy ones.
  client, _ := grpc.Dial("consul://10.10.0.1:1234/user-service?scheme=https&tags=primary,eu&health=fallbackToUnhealthy")

  // Instantiates a GRPC client with a round-robin load-balancer.
  // The addresses of the service "metrics" are resolved via the default
  // consul server "http://127.0.01:8500".
  client, _ = grpc.Dial("consul://metrics", grpc.WithBalancerName("round_robin"))
}
```
