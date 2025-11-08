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
	Sys() interface{}
}

// Metadata метаданные файла
type Metadata map[string]any
