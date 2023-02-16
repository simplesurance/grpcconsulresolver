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
//  token				  - Consul token used in API requests
// Defaults:
//            consul-server:		127.0.0.1:8500
//            scheme:			http
//            tags:			nil
//            health:			healthy
//            token:
// Example: consul://localhost:1234/user-service?scheme=https&tags=primary,eu
// Will connect to the consul server localhost:1234 via https and lookup the
// address of the service with the name "user-service" and the tags "primary"
// and "eu".
// If an OPT is defined multiple times, only the value of the last occurrence is
// used.
import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/grpc/resolver"
)

type resolverBuilder struct{}

const scheme = "consul"

// NewBuilder returns a builder for a consul resolver.
func NewBuilder() resolver.Builder {
	return &resolverBuilder{}
}

func extractOpts(opts url.Values) (scheme string, tags []string, health healthFilter, token string, err error) {
	for key, values := range opts {
		if len(values) == 0 {
			continue
		}
		value := values[len(values)-1]

		switch strings.ToLower(key) {
		case "scheme":
			scheme = strings.ToLower(value)
			if scheme != "http" && scheme != "https" {
				return "", nil, healthFilterUndefined, "", fmt.Errorf("unsupported scheme '%s'", value)
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
				return "", nil, healthFilterUndefined, "", fmt.Errorf("unsupported health parameter value: '%s'", value)
			}
		case "token":
			token = value

		default:
			return "", nil, healthFilterUndefined, "", fmt.Errorf("unsupported parameter: '%s'", key)
		}
	}

	return scheme, tags, health, token, err
}

func parseEndpoint(url *url.URL) (serviceName, scheme string, tags []string, health healthFilter, token string, err error) {
	const defScheme = "http"
	const defHealthFilter = healthFilterOnlyHealthy

	// url.Path contains a leading "/", when the URL is in the form
	// scheme://host/path, remove it
	serviceName = strings.TrimPrefix(url.Path, "/")
	if serviceName == "" {
		return "", "", nil, health, "", errors.New("path is missing in url")
	}

	scheme, tags, health, token, err = extractOpts(url.Query())
	if err != nil {
		return "", "", nil, health, "", err
	}

	if scheme == "" {
		scheme = defScheme
	}

	if health == healthFilterUndefined {
		health = defHealthFilter
	}

	return serviceName, scheme, tags, health, token, nil
}

func (*resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	serviceName, scheme, tags, health, token, err := parseEndpoint(&target.URL)
	if err != nil {
		return nil, err
	}

	r, err := newConsulResolver(cc, scheme, target.URL.Host, serviceName, tags, health, token)
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
