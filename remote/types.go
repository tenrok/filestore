package remote

import (
	"os"
	"time"
)

// FileInfo реализует интерфейс os.FileInfo.
type FileInfo interface {
	Name() string
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
	Sys() any
}

// Metadata метаданные файла
type Metadata map[string]any

// ListResult содержит страницу файлов и токен для следующей страницы.
type ListResult struct {
	Files     []FileInfo
	NextToken string // пустая строка, если больше нет страниц
}
