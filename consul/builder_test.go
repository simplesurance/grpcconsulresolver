package consul

import (
	"reflect"
	"testing"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint        string
		wantServiceName string
		wantScheme      string
		wantTags        []string
		wantErr         bool
	}{
		{
			"user-service-rpc?scheme=https&tags=primary,backup",
			"user-service-rpc",
			"https",
			[]string{"primary", "backup"},
			false,
		},

		{
			"user-service-rpc?tags=pri-mary,backup&scheme=http",
			"user-service-rpc",
			"http",
			[]string{"pri-mary", "backup"},
			false,
		},

		{
			"user-service-rpc",
			"user-service-rpc",
			"http",
			nil,
			false,
		},

		{
			"user-service-rpc?scheme=ftp",
			"",
			"",
			nil,
			true,
		},

		{
			"user-service-rpc?scheme=http?tags=primary",
			"",
			"",
			nil,
			true,
		},

		{
			"user-service-rpc?unsupportedparam=yo",
			"",
			"",
			nil,
			true,
		},

		{
			"",
			"",
			"",
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			serviceName, scheme, tags, err := parseEndpoint(tt.endpoint)
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
		})
	}
}
