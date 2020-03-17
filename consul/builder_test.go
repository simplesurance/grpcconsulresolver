package consul

import (
	"reflect"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint         string
		wantServiceName  string
		wantScheme       string
		wantTags         []string
		wantErr          bool
		wantHealthFilter healthFilter
	}{
		{
			"user-service-rpc?scheme=https&tags=primary,backup&health=healthy",
			"user-service-rpc",
			"https",
			[]string{"primary", "backup"},
			false,
			healthFilterOnlyHealthy,
		},

		{
			"user-service-rpc?tags=pri-mary,backup&scheme=http&health=fallbackToUnhealthy",
			"user-service-rpc",
			"http",
			[]string{"pri-mary", "backup"},
			false,
			healthFilterFallbackToUnhealthy,
		},

		{
			"user-service-rpc",
			"user-service-rpc",
			"http",
			nil,
			false,
			healthFilterOnlyHealthy,
		},

		{
			"user-service-rpc?health=blablub",
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			"user-service-rpc?scheme=ftp",
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			"user-service-rpc?scheme=http?tags=primary",
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			"user-service-rpc?unsupportedparam=yo",
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},

		{
			"",
			"",
			"",
			nil,
			true,
			healthFilterUndefined,
		},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
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
