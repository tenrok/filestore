package s3storage

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewConfig_Schemes(t *testing.T) {
	cases := []struct {
		name       string
		connString string
		expected   *Config
	}{
		{
			name:       "s3 basic",
			connString: "s3://AK:SK@example.com/bucket",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
		},
		{
			name:       "s3 with all params",
			connString: "s3://AK:SK@example.com:9443/bucket/prefix?secure=1&region=us-west-1",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com:9443",
				AccessKey: "AK",
				SecretKey: "SK",
				Bucket:    "bucket",
				Prefix:    "prefix",
				Region:    "us-west-1",
				Secure:    true,
			},
		},
		{
			name:       "minio alias basic",
			connString: "minio://AK:SK@example.com/bucket",
			expected: &Config{
				Scheme:    "minio",
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
		},
		{
			name:       "minio alias with all params",
			connString: "minio://AK:SK@example.com:9443/bucket/prefix?secure=1&region=us-west-1",
			expected: &Config{
				Scheme:    "minio",
				Endpoint:  "example.com:9443",
				AccessKey: "AK",
				SecretKey: "SK",
				Bucket:    "bucket",
				Prefix:    "prefix",
				Region:    "us-west-1",
				Secure:    true,
			},
		},
		{
			name:       "minio with session token",
			connString: "minio://AK:SK@example.com/bucket?token=STSTOKEN",
			expected: &Config{
				Scheme:    "minio",
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Token:     "STSTOKEN",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
		},
		{
			name:       "s3 with session token",
			connString: "s3://AK:SK@example.com/bucket?token=STSTOKEN",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Token:     "STSTOKEN",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := NewConfig(tc.connString)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(cfg, tc.expected) {
				t.Errorf("expected %+v, got %+v", tc.expected, cfg)
			}
		})
	}
}

func TestNewConfig_UnsupportedScheme(t *testing.T) {
	cases := []string{
		"http://example.com/bucket",
		"https://example.com/bucket",
		"ftp://example.com/bucket",
		"gcs://example.com/bucket",
		"",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			_, err := NewConfig(in)
			if err == nil {
				t.Fatalf("expected error for %q", in)
			}
		})
	}
}

func TestConnString_RoundTrip(t *testing.T) {
	cases := []string{
		"s3://AK:SK@example.com/bucket?region=us-east-1",
		"s3://AK:SK@example.com:9443/bucket/prefix?region=us-east-1&secure=1",
		"s3://AK:SK@example.com/bucket/prefix?region=us-east-1&secure=1&token=STS",
		"minio://AK:SK@example.com/bucket?region=us-east-1",
		"minio://AK:SK@example.com:9443/bucket/prefix?region=us-east-1&secure=1",
		"minio://AK:SK@example.com:9443/bucket/prefix?region=us-east-1&secure=1&token=STS",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			cfg, err := NewConfig(in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			out := ConnString(*cfg)
			if out != in {
				t.Errorf("round-trip mismatch:\n  in:  %s\n  out: %s", in, out)
			}
		})
	}
}

func TestRedactedString(t *testing.T) {
	cfg := Config{
		Scheme:    "s3",
		Endpoint:  "example.com",
		AccessKey: "AKIA",
		SecretKey: "SUPERSECRET",
		Bucket:    "bucket",
		Region:    "us-east-1",
	}
	got := RedactedString(cfg)
	if want := "s3://AKIA:****@example.com/bucket?region=us-east-1"; got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
	if contains(got, "SUPERSECRET") {
		t.Errorf("redacted string leaks secret: %q", got)
	}
}

func TestRedactedString_SchemePreserved(t *testing.T) {
	cases := []struct {
		name   string
		scheme string
	}{
		{name: "s3", scheme: "s3"},
		{name: "minio", scheme: "minio"},
		{name: "empty defaults to s3", scheme: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Scheme:    tc.scheme,
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SUPERSECRET",
				Bucket:    "bucket",
				Region:    "us-east-1",
			}
			got := RedactedString(cfg)

			wantScheme := tc.scheme
			if wantScheme == "" {
				wantScheme = "s3"
			}
			if !contains(got, wantScheme+"://") {
				t.Errorf("expected scheme %q in %q", wantScheme, got)
			}
			if contains(got, "SUPERSECRET") {
				t.Errorf("redacted string leaks secret: %q", got)
			}
		})
	}
}

func TestRedactedString_TokenMasked(t *testing.T) {
	cfg := Config{
		Scheme:    "minio",
		Endpoint:  "example.com",
		AccessKey: "AK",
		SecretKey: "SK",
		Token:     "VERYSECRETTOKEN",
		Bucket:    "bucket",
		Region:    "us-east-1",
	}
	got := RedactedString(cfg)
	if contains(got, "VERYSECRETTOKEN") {
		t.Errorf("redacted string leaks token: %q", got)
	}
	if !contains(got, "token=%2A%2A%2A%2A") && !contains(got, "token=****") {
		t.Errorf("expected masked token in %q", got)
	}
}

func TestNewConfig_MinioBackwardCompatible(t *testing.T) {
	in := "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@play.min.io:9443/bucket/prefix?secure=1&region=us-west-1"

	cfg, err := NewConfig(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	want := &Config{
		Scheme:    "minio",
		Endpoint:  "play.min.io:9443",
		AccessKey: "e5JXrHpr093RVk1s9IcE",
		SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
		Bucket:    "bucket",
		Prefix:    "prefix",
		Region:    "us-west-1",
		Secure:    true,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("expected %+v, got %+v", want, cfg)
	}
}

func TestNewConfig(t *testing.T) {
	cases := []struct {
		name        string
		connString  string
		expected    *Config
		expectedErr error
	}{
		{
			name:       "Test 1",
			connString: "s3://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com/bucket",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
		},
		{
			name:       "Test 2",
			connString: "s3://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com:9443/bucket?secure=1&region=us-west-1",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com:9443",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Region:    "us-west-1",
				Secure:    true,
			},
		},
		{
			name:       "Test 3",
			connString: "s3://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com:9443/bucket/prefix?secure=1",
			expected: &Config{
				Scheme:    "s3",
				Endpoint:  "example.com:9443",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Prefix:    "prefix",
				Region:    "us-east-1",
				Secure:    true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := NewConfig(tc.connString)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
			if !reflect.DeepEqual(cfg, tc.expected) {
				t.Errorf("expected %+v, got %+v", tc.expected, cfg)
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
				Scheme:    "s3",
				Endpoint:  "example.com",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
			expected: "s3://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com/bucket?region=us-east-1",
		},
		{
			name: "Test 2",
			cfg: Config{
				Scheme:    "s3",
				Endpoint:  "example.com:9443",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Prefix:    "prefix",
				Region:    "us-east-1",
				Secure:    true,
			},
			expected: "s3://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com:9443/bucket/prefix?region=us-east-1&secure=1",
		},
		{
			name: "Test 3 (minio scheme preserved)",
			cfg: Config{
				Scheme:    "minio",
				Endpoint:  "example.com:9443",
				AccessKey: "e5JXrHpr093RVk1s9IcE",
				SecretKey: "QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ",
				Bucket:    "bucket",
				Prefix:    "prefix",
				Region:    "us-east-1",
				Secure:    true,
			},
			expected: "minio://e5JXrHpr093RVk1s9IcE:QJSEqip9gX2b3041deUR1K5BCJjSDubYwy48K3SQ@example.com:9443/bucket/prefix?region=us-east-1&secure=1",
		},
		{
			name: "Test 4 (empty scheme defaults to s3)",
			cfg: Config{
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
			expected: "s3://AK:SK@example.com/bucket?region=us-east-1",
		},
		{
			name: "Test 5 (token in query)",
			cfg: Config{
				Scheme:    "minio",
				Endpoint:  "example.com",
				AccessKey: "AK",
				SecretKey: "SK",
				Token:     "STS",
				Bucket:    "bucket",
				Region:    "us-east-1",
			},
			expected: "minio://AK:SK@example.com/bucket?region=us-east-1&token=STS",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ConnString(tc.cfg); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
