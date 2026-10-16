package s3storage

import (
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// Config описывает параметры подключения к S3-совместимому хранилищу.
//
// Scheme хранится, чтобы обеспечить обратную совместимость для исторического алиаса "minio://". Канонический scheme — "s3".
type Config struct {
	Scheme    string // "s3" (по умолчанию) или "minio" (устаревший алиас)
	Endpoint  string
	AccessKey string
	SecretKey string
	Token     string // session token; для обратной совместимости с minio://?token=
	Bucket    string
	Prefix    string
	Region    string
	Secure    bool
}

// NewConfig парсит строку подключения вида
//
//	s3://access:secret@endpoint[:port]/bucket[/prefix][?secure=1&region=...&token=...]
//	minio://access:secret@endpoint[:port]/bucket[/prefix][?secure=1&region=...&token=...]
//
// "minio" — устаревший алиас, эквивалентный "s3". Он поддерживается, чтобы старые строки подключения продолжали работать после удаления miniostorage.
func NewConfig(connString string) (*Config, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "s3", "minio":
		// ok
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	cfg := &Config{
		Scheme:   u.Scheme,
		Endpoint: u.Host,
		Region:   "us-east-1",
		Token:    u.Query().Get("token"),
	}

	if u.User != nil {
		cfg.AccessKey = u.User.Username()
		cfg.SecretKey, _ = u.User.Password()
	}

	if u.Query().Has("secure") {
		secure, err := strconv.ParseBool(u.Query().Get("secure"))
		if err != nil {
			return nil, fmt.Errorf("invalid secure parameter: %w", err)
		}
		cfg.Secure = secure
	}
	if u.Query().Has("region") {
		cfg.Region = u.Query().Get("region")
	}

	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
	cfg.Bucket = parts[0]
	if len(parts) > 1 {
		cfg.Prefix = parts[1]
	}

	return cfg, nil
}

// scheme возвращает scheme для сериализации: по умолчанию "s3".
func (c Config) scheme() string {
	if c.Scheme == "" {
		return "s3"
	}
	return c.Scheme
}

// ConnString восстанавливает строку подключения, включая секрет.
// Сохраняет исходную scheme (s3 или minio).
// Используйте только для хранения/передачи, не для логирования.
func ConnString(cfg Config) string {
	params := url.Values{}
	if cfg.Region != "" {
		params.Add("region", cfg.Region)
	}
	if cfg.Secure {
		params.Add("secure", "1")
	}
	if cfg.Token != "" {
		params.Add("token", cfg.Token)
	}

	u := url.URL{
		Scheme:   cfg.scheme(),
		Host:     cfg.Endpoint,
		User:     url.UserPassword(cfg.AccessKey, cfg.SecretKey),
		Path:     path.Join("/", cfg.Bucket, cfg.Prefix),
		RawQuery: params.Encode(),
	}

	return u.String()
}

// RedactedString возвращает строку подключения с маскированным секретом.
// Безопасна для логов. Сохраняет исходную scheme.
//
// Реализация собирает userinfo вручную, потому что url.UserPassword
// экранирует '*' → '%2A' и ':' → '%3A', что делает строку нечитаемой:
// "s3://AKIA%3A%2A%2A%2A%2A@host/bucket" вместо "s3://AKIA:****@host/bucket".
func RedactedString(cfg Config) string {
	params := url.Values{}
	if cfg.Region != "" {
		params.Add("region", cfg.Region)
	}
	if cfg.Secure {
		params.Add("secure", "1")
	}
	// token тоже не логируем
	if cfg.Token != "" {
		params.Add("token", "****")
	}

	u := url.URL{
		Scheme:   cfg.scheme(),
		Host:     cfg.Endpoint,
		Path:     path.Join("/", cfg.Bucket, cfg.Prefix),
		RawQuery: params.Encode(),
	}

	// Без access key — просто обычная строка без userinfo.
	if cfg.AccessKey == "" {
		return u.String()
	}

	// url.User корректно экранирует сам логин по правилам encodeUserPassword,
	// а ":****" добавляем как литерал, минуя escape.
	userinfo := url.User(cfg.AccessKey).String()
	if cfg.SecretKey != "" {
		userinfo += ":****"
	}

	s := u.Scheme + "://" + userinfo + "@" + u.Host + u.EscapedPath()
	if u.RawQuery != "" {
		s += "?" + u.RawQuery
	}
	return s
}
