package consul

// Package consul implements a GRPC resolver for consul service discovery.
// The resolver queries consul for healthy services with a specified name.
// Blocking Consul queries
// (https://www.consul.io/api/index.html#blocking-queries) are used to monitor
// consul for changes.
//
// To register the resolver with the GRPC-Go library run:
//	resolver.Register(consul.NewBuilder())
// Afterwards it can be used by calling grpc.Dial() and passing an URI in the
// following format: consul://[<consul-server>]/<serviceName>[?<OPT>[&<OPT>]]
// where OPT:
//  scheme=(http|https)			  - establish connection to consul via
//  http or https
//  tags=<tag>[,<tag>]...		  - filters the consul service by tags
//  health=(healthy|fallbackToUnhealthy)  - filter services by their
//					    health checks status.
//					    fallbackToUnhealthy resolves to
//					    unhealthy ones if no healthy ones
//					    are available.
// Defaults:
//            consul-server:		127.0.0.1:8500
//            scheme:			http
//            tags:			nil
//	      health:			healthy
// Example: consul://localhost:1234/user-service?scheme=https&tags=primary,eu
// Will connect to the consul server localhost:1234 via https and lookup the
// address of the service with the name "user-service" and the tags "primary"
// and "eu".

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/resolver"
)

type resolverBuilder struct{}

const scheme = "consul"

// NewBuilder returns a builder for a consul resolver.
func NewBuilder() resolver.Builder {
	return &resolverBuilder{}
}

func endpointParts(endpoint string) (serviceName, opts string, err error) {
	spl := strings.Split(endpoint, "?")
	if len(spl) == 1 {
		return spl[0], "", nil
	}

	if len(spl) == 2 {
		return spl[0], spl[1], nil
	}

	return "", "", errors.New("endpoint contains multiple '?' characters")
}

func splitOpts(opts string) ([]string, error) {
	const maxParams = 3

	spl := strings.Split(opts, "&")
	if len(spl) <= maxParams {
		return spl, nil
	}

	return nil, fmt.Errorf("endpoint can only contain <=%d parameters", maxParams)
}

func splitOptKV(opt string) (key, value string, err error) {
	spl := strings.Split(opt, "=")
	if len(spl) == 2 {
		return spl[0], spl[1], nil
	}

	return "", "", errors.New("parameter must contain a single '='")
}

func extractOpts(opts string) (scheme string, tags []string, health healthFilter, err error) {
	optsSl, err := splitOpts(opts)
	if err != nil {
		return "", nil, health, err
	}

	for _, opt := range optsSl {
		key, value, err := splitOptKV(opt)
		if err != nil {
			return "", nil, healthFilterUndefined, err
		}

		switch key = strings.ToLower(key); key {
		case "scheme":
			scheme = strings.ToLower(value)
			if scheme != "http" && scheme != "https" {
				return "", nil, healthFilterUndefined, fmt.Errorf("unsupported scheme '%s'", scheme)
			}

		case "tags":
			tags = strings.Split(value, ",")

		case "health":
			switch strings.ToLower(value) {
			case "healthy":
				health = healthFilterOnlyHealthy
			case "fallbacktounhealthy":
				health = healthFilterFallbackToUnhealthy
			default:
				return "", nil, healthFilterUndefined, fmt.Errorf("unsupported health parameter value: '%s'", value)
			}

		default:
			return "", nil, healthFilterUndefined, fmt.Errorf("unsupported parameter: '%s'", key)
		}
	}

	return scheme, tags, health, err
}

func parseEndpoint(endpoint string) (serviceName, scheme string, tags []string, health healthFilter, err error) {
	const defScheme = "http"
	const defHealthFilter = healthFilterOnlyHealthy

	serviceName, opts, err := endpointParts(endpoint)
	if err != nil {
		return "", "", nil, health, err
	}

	if serviceName == "" {
		return "", "", nil, health, errors.New("endpoint is empty")
	}

	if opts == "" {
		return serviceName, defScheme, nil, defHealthFilter, nil
	}

	scheme, tags, health, err = extractOpts(opts)
	if err != nil {
		return "", "", nil, health, err
	}

	if scheme == "" {
		scheme = defScheme
	}

	if health == healthFilterUndefined {
		health = defHealthFilter
	}

	return serviceName, scheme, tags, health, nil
}

func (*resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	serviceName, scheme, tags, health, err := parseEndpoint(target.Endpoint)
	if err != nil {
		return nil, err
	}

	r, err := newConsulResolver(cc, scheme, target.Authority, serviceName, tags, health)
	if err != nil {
		return nil, err
	}

	r.start()

	return r, nil
}

// Scheme returns the URI scheme for the resolver
func (*resolverBuilder) Scheme() string {
	return scheme
}
