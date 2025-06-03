package remote

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
)

var (
	storagesMu sync.RWMutex
	storages   = make(map[string]Storage)
)

type Storage interface {
	NewStorage(ctx context.Context, connString string) (Storage, error)
	Create(name string) (http.File, error)
	Open(name string) (http.File, error)
	OpenFile(name string, flag int, fileMode os.FileMode) (http.File, error)
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldName, newName string) error
	Stat(name string) (os.FileInfo, error)
}

// NewStorage returns a new remote storage instance.
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

// Register globally registers a storage
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
