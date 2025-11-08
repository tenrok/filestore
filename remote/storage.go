package remote

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
)

var (
	storagesMu sync.RWMutex
	storages   = make(map[string]Storage)
)

type Uploader interface {
	Upload(path string, reader io.Reader, opts ...Option) error
}

type Storage interface {
	NewStorage(ctx context.Context, connString string) (Storage, error)

	// Open реализует метод http.FileSystem.
	Open(name string) (http.File, error)

	// Create создаёт файл и возвращает io.WriteCloser.
	Create(name string, opts ...Option) (io.WriteCloser, error)

	// Remove удаляет файл.
	Remove(name string) error

	// Stat получает информацию о файле/каталоге.
	Stat(name string) (FileInfo, error)

	// Exists определяет, существует ли файл или каталог.
	Exists(name string) (bool, error)

	// IsDir определяет, является ли путь каталогом.
	IsDir(name string) (bool, error)

	// IsFile определяет, является ли путь файлом.
	IsFile(name string) (bool, error)

	Uploader() Uploader
}

// NewStorage создаёт новый экземпляр удаленного хранилища.
func NewStorage(ctx context.Context, connString string) (Storage, error) {
	scheme, err := schemeFromURL(connString)
	if err != nil {
		return nil, err
	}

	storagesMu.RLock()
	s, ok := storages[scheme]
	storagesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown storage %v (forgotten import?)", scheme)
	}

	return s.NewStorage(ctx, connString)
}

// Register глобально регистрирует хранилище.
func Register(name string, storage Storage) {
	storagesMu.Lock()
	defer storagesMu.Unlock()

	if storage == nil {
		panic("Register storage is nil")
	}

	if _, exists := storages[name]; exists {
		panic("Register called twice for storage " + name)
	}

	storages[name] = storage
}
