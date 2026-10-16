package s3storage

import "strings"

const (
	defaultPageSize = 100
	maxPageSize     = 1000 // ограничение S3 ListObjectsV2
)

// joinPrefix собирает S3-ключ из префикса и имени без нормализации path.Join
// (которая схлопывает "//", убирает ".." и возвращает "." на пустых входных).
func joinPrefix(prefix, name string) string {
	switch {
	case prefix == "":
		return name
	case name == "":
		return prefix
	default:
		return strings.TrimRight(prefix, "/") + "/" + strings.TrimLeft(name, "/")
	}
}

// storagePrefix возвращает нормализованный префикс хранилища с гарантированным
// завершающим слэшем (пустая строка — если префикс не задан).
func storagePrefix(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// endpointURL добавляет схему к endpoint в соответствии с флагом secure,
// если схема ещё не указана явно.
func endpointURL(endpoint string, secure bool) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	if secure {
		return "https://" + endpoint
	}
	return "http://" + endpoint
}
