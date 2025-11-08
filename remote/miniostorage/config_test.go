package miniostorage

import (
	"reflect"
	"testing"
)

func TestNewConfig(t *testing.T) {
	cases := []struct {
		name        string
		connString  string
		expected    *Config
		expectedErr error
	}{
		{
			name:       "Test 1",
			connString: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io/bucket",
			expected: &Config{
				Endpoint:    "play.min.io",
				AccessKeyID: "e5JXrHpr093RVk1s9IcE",
				SecretKey:   "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				BucketName:  "bucket",
				Prefix:      "",
				Region:      "us-east-1",
				Secure:      false,
			},
		},
		{
			name:       "Test 2",
			connString: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io:9443/bucket?secure=1&region=us-west-1",
			expected: &Config{
				Endpoint:    "play.min.io:9443",
				AccessKeyID: "e5JXrHpr093RVk1s9IcE",
				SecretKey:   "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				BucketName:  "bucket",
				Prefix:      "",
				Region:      "us-west-1",
				Secure:      true,
			},
		},
		{
			name:       "Test 3",
			connString: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io:9443/bucket/prefix?secure=1",
			expected: &Config{
				Endpoint:    "play.min.io:9443",
				AccessKeyID: "e5JXrHpr093RVk1s9IcE",
				SecretKey:   "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				BucketName:  "bucket",
				Prefix:      "prefix",
				Region:      "us-east-1",
				Secure:      true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := NewConfig(tc.connString)
			if err != nil {
				if err != tc.expectedErr {
					t.Fatalf("Expected %q, got %q", tc.expectedErr, err)
				}
			}

			if !reflect.DeepEqual(cfg, tc.expected) {
				t.Errorf("expected %v, got %v", cfg, tc.expected)
			}
		})
	}
}

func TestConnString(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		expected string
	}{
		{
			name: "Test 1",
			cfg: Config{
				Endpoint:    "play.min.io",
				AccessKeyID: "e5JXrHpr093RVk1s9IcE",
				SecretKey:   "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				BucketName:  "bucket",
				Prefix:      "",
				Region:      "us-east-1",
				Secure:      false,
			},
			expected: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io/bucket?region=us-east-1",
		},
		{
			name: "Test 2",
			cfg: Config{
				Endpoint:    "play.min.io:9443",
				AccessKeyID: "e5JXrHpr093RVk1s9IcE",
				SecretKey:   "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				BucketName:  "bucket",
				Prefix:      "prefix",
				Region:      "us-east-1",
				Secure:      true,
			},
			expected: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io:9443/bucket/prefix?region=us-east-1&secure=1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			str := ConnString(tc.cfg)
			if str != tc.expected {
				t.Errorf("expected %q, got %q", str, tc.expected)
			}
		})
	}
}
