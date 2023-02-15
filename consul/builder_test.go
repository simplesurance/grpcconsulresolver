package consul

import (
	"net/url"
	"reflect"
	"testing"
)

func mustParseURL(t *testing.T, strURL string) *url.URL {
	result, err := url.Parse(strURL)
	if err != nil {
		t.Fatal(err)
	}

	return result
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint         *url.URL
		wantServiceName  string
		wantScheme       string
		wantTags         []string
		wantErr          bool
		wantHealthFilter healthFilter
	}{
		{
			mustParseURL(t, "consul://127.0.01:8500/user-service-rpc?scheme=https&tags=primary,backup&health=healthy"),
			"user-service-rpc",
			"https",
			[]string{"primary", "backup"},
			false,
			healthFilterOnlyHealthy,
		},

		{
			mustParseURL(t, "consul://127.0.0.1/user-service-rpc?tags=pri-mary,backup&scheme=http&health=fallbackToUnhealthy"),
			"user-service-rpc",
			"http",
			[]string{"pri-mary", "backup"},
			false,
			healthFilterFallbackToUnhealthy,
		},

		{
			mustParseURL(t, "consul://localhost/user-service-rpc"),
			"user-service-rpc",
			"http",
			nil,
			false,
			healthFilterOnlyHealthy,
		},

		{
			mustParseURL(t, "consul://consul/user-service-rpc?health=blablub"),
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			mustParseURL(t, "consul://consul:8500/user-service-rpc?scheme=ftp"),
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			mustParseURL(t, "consul://[::1]/user-service-rpc?scheme=http?tags=primary"),
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			mustParseURL(t, "consul://localhost/user-service-rpc?unsupportedparam=yo"),
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			mustParseURL(t, "consul://127.0.01:8500/user-service-rpc?scheme=http&scheme=https&tags=primary,backup&health=healthy&tags=secondary&health=fallbacktounhealthy"),
			"user-service-rpc",
			"https",
			[]string{"secondary"},
			false,
			healthFilterFallbackToUnhealthy,
		},

		{
			mustParseURL(t, ""),
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint.String(), func(t *testing.T) {
			serviceName, scheme, tags, healthFilter, err := parseEndpoint(tt.endpoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseEndpoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if serviceName != tt.wantServiceName {
				t.Errorf("parseEndpoint() gotServiceName = %v, want %v", serviceName, tt.wantServiceName)
			}

			if scheme != tt.wantScheme {
				t.Errorf("parseEndpoint() gotScheme = %v, want %v", scheme, tt.wantScheme)
			}

			if !reflect.DeepEqual(tt.wantTags, tags) {
				t.Errorf("parseEndpoint() gotTags = %+v, want %+v", tags, tt.wantTags)
			}

			if healthFilter != tt.wantHealthFilter {
				t.Errorf("parseEndpoint() gotHealthFilter = %v, want %v", healthFilter, tt.wantHealthFilter)
			}
		})
	}
}
