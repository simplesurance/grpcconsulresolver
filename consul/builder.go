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
// following format: consul://[<consul-server>]/<serviceName>[?<OPT>[&<OPT>]
// where OPT:
//            scheme=(http|https)   - to define if the connection to consul is
//				      established via http or https
//            tags=<tag>[,<tag>]... - specifies that the consul service entry must have
//                                    one of those tags
// Defaults:
//            consul-server: http://127.0.0.1:8500
//            scheme:	     http
//            tags:          nil
// Example: consul://localhost:1234/user-service?scheme=https&tags=primary,eu
// Will connect the consul server localhost:1234 via https and looksup the
// adress of the service with the name "user-service" and the tag "primary" or
// "eu".

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
	spl := strings.Split(opts, "&")
	if len(spl) <= 2 {
		return spl, nil
	}

	return nil, errors.New("endpoint can only contain <=2 parameters")
}

func splitOptKV(opt string) (key, value string, err error) {
	spl := strings.Split(opt, "=")
	if len(spl) == 2 {
		return spl[0], spl[1], nil
	}

	return "", "", errors.New("parameter must contain a single '='")
}

func extractOpts(opts string) (scheme string, tags []string, err error) {
	optsSl, err := splitOpts(opts)
	if err != nil {
		return "", nil, err
	}

	for _, opt := range optsSl {
		key, value, err := splitOptKV(opt)
		if err != nil {
			return "", nil, err
		}

		switch key = strings.ToLower(key); key {
		case "scheme":
			scheme = strings.ToLower(value)
			if scheme != "http" && scheme != "https" {
				return "", nil, fmt.Errorf("unsupported scheme '%s'", scheme)
			}

		case "tags":
			tags = strings.Split(value, ",")

		default:
			return "", nil, fmt.Errorf("unsupported parameter: '%s'", key)
		}
	}

	return
}

func parseEndpoint(endpoint string) (serviceName, scheme string, tags []string, err error) {
	const defScheme = "http"

	serviceName, opts, err := endpointParts(endpoint)
	if err != nil {
		return "", "", nil, err
	}
	if serviceName == "" {
		return "", "", nil, errors.New("endpoint is empty")
	}
	if opts == "" {
		return serviceName, defScheme, nil, nil
	}

	scheme, tags, err = extractOpts(opts)
	if err != nil {
		return "", "", nil, err
	}

	if scheme == "" {
		return serviceName, defScheme, tags, nil
	}

	return
}

func (*resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOption) (resolver.Resolver, error) {
	serviceName, scheme, tags, err := parseEndpoint(target.Endpoint)
	if err != nil {
		return nil, err
	}

	r, err := newConsulResolver(cc, scheme, target.Authority, serviceName, tags)
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
