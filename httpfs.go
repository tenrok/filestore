package filestore

import (
	"net/http"
	"strings"

	"github.com/tenrok/filestore/remote"
)

// Убеждаемся в том, что мы всегда реализуем интерфейс http.FileSystem.
var _ http.FileSystem = (*HttpFS)(nil)

type HttpFS struct {
	localStorage  *LocalStorage
	remoteStorage remote.Storage
}

type HttpFSOption func(*HttpFS)

// WithRemoteStorage
func WithRemoteStorage(storage remote.Storage) HttpFSOption {
	return func(f *HttpFS) {
		f.remoteStorage = storage
	}
}

// NewHttpFS создаёт новый экземпляр файловой системы.
func NewHttpFS(rootDir string, opts ...HttpFSOption) (*HttpFS, error) {
	localStorage, err := NewLocalStorage(rootDir)
	if err != nil {
		return nil, err
	}

	f := &HttpFS{}
	f.localStorage = localStorage

	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}

	return f, nil
}

// Open реализует метод http.FileSystem.
func (f *HttpFS) Open(name string) (http.File, error) {
	name = strings.TrimPrefix(name, "/")
	if f.remoteStorage != nil {
		return f.remoteStorage.Open(name)
	}
	return f.localStorage.Open(name)
}

// Remove удаляет файл.
func (f *HttpFS) Remove(name string) error {
	name = strings.TrimPrefix(name, "/")
	if f.remoteStorage != nil {
		return f.remoteStorage.Remove(name)
	}
	return f.localStorage.Remove(name)
}

// LocalStorage возвращает указатель на локальное хранилище.
func (f *HttpFS) LocalStorage() *LocalStorage { return f.localStorage }

// RemoteStorage возвращает указатель на удалённое хранилище.
func (f *HttpFS) RemoteStorage() remote.Storage { return f.remoteStorage }
