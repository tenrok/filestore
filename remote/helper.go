package remote

import (
	"errors"
	"strings"
)

var (
	ErrNoScheme = errors.New("no scheme")
	ErrEmptyURL = errors.New("URL cannot be empty")
)

// schemeFromURL возвращает схему из строки URL.
func schemeFromURL(url string) (string, error) {
	if url == "" {
		return "", ErrEmptyURL
	}
	i := strings.Index(url, ":")
	if i < 1 {
		return "", ErrNoScheme
	}
	return url[:i], nil
}
