package filestore

import (
	"net/http"
	"strings"

	"github.com/tenrok/filestore/remote"
)

var _ http.FileSystem = (*HttpFS)(nil)

type HttpFS struct {
	store         *Store
	remoteStorage remote.Storage
}

type HttpFSOption func(*HttpFS)

// WithRemoteStorage
func WithRemoteStorage(storage remote.Storage) HttpFSOption {
	return func(httpFS *HttpFS) {
		httpFS.remoteStorage = storage
	}
}

// NewHttpFS
func NewHttpFS(dir string, opts ...HttpFSOption) (*HttpFS, error) {
	store, err := NewStore(dir)
	if err != nil {
		return nil, err
	}

	f := &HttpFS{store: store}

	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}

	return f, nil
}

// Open
func (f *HttpFS) Open(name string) (http.File, error) {
	n := strings.TrimPrefix(name, "/")

	if f.remoteStorage != nil {
		return f.remoteStorage.Open(name)
	}

	return f.store.Open(n)
}

// RemoteStorage
func (f *HttpFS) RemoteStorage() remote.Storage {
	return f.remoteStorage
}
