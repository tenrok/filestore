package filestore

import (
	"os"
	"time"

	"github.com/tenrok/filestore/remote"
)

// Проверка на этапе компиляции, что тип реализует remote.FileInfo.
var _ remote.FileInfo = (*localFileInfo)(nil)

// localFileInfo реализует remote.FileInfo для файлов локального хранилища.
type localFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (f *localFileInfo) Name() string       { return f.name }
func (f *localFileInfo) Size() int64        { return f.size }
func (f *localFileInfo) Mode() os.FileMode  { return f.mode }
func (f *localFileInfo) ModTime() time.Time { return f.modTime }
func (f *localFileInfo) IsDir() bool        { return f.isDir }
func (f *localFileInfo) Sys() any           { return nil }

// из os.FileInfo
func newLocalFileInfo(name string, info os.FileInfo) *localFileInfo {
	return &localFileInfo{
		name:    name,
		size:    info.Size(),
		mode:    info.Mode(),
		modTime: info.ModTime(),
		isDir:   info.IsDir(),
	}
}
