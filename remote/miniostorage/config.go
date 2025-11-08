package miniostorage

import (
	"net/url"
	"path"
	"strconv"
	"strings"
)

type Config struct {
	Endpoint    string
	AccessKeyID string
	SecretKey   string
	Token       string
	BucketName  string
	Prefix      string
	Region      string
	Secure      bool
}

// NewConfig парсирует строку подключения
func NewConfig(connString string) (*Config, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}

	queries := u.Query()
	var accessKeyID, secretKey string
	if u.User != nil {
		accessKeyID = u.User.Username()
		if s, ok := u.User.Password(); ok {
			secretKey = s
		}
	}
	token := queries.Get("token")

	cfg := &Config{}
	cfg.Endpoint = u.Host
	cfg.AccessKeyID = accessKeyID
	cfg.SecretKey = secretKey
	cfg.Token = token
	if queries.Has("secure") {
		secure, err := strconv.ParseBool(queries.Get("secure"))
		if err != nil {
			return nil, err
		}
		cfg.Secure = secure
	}
	if queries.Has("region") {
		cfg.Region = queries.Get("region")
	} else {
		cfg.Region = "us-east-1"
	}

	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
	cfg.BucketName = parts[0]
	if len(parts) > 1 {
		cfg.Prefix = parts[1]
	}

	return cfg, nil
}

func ConnString(cfg Config) string {
	params := url.Values{}
	if cfg.Region != "" {
		params.Add("region", cfg.Region)
	}
	if cfg.Secure {
		params.Add("secure", "1")
	}
	u := url.URL{
		Scheme:   "minio",
		Host:     cfg.Endpoint,
		User:     url.UserPassword(cfg.AccessKeyID, cfg.SecretKey),
		Path:     path.Join("/", cfg.BucketName, cfg.Prefix),
		RawQuery: params.Encode(),
	}
	return u.String()
}
