package miniostorage

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const folderSize = 42

var _ fs.FileInfo = (*MinioFileInfo)(nil)

type MinioFileInfo struct {
	ETag     string
	name     string
	size     int64
	updated  time.Time
	isDir    bool
	fileMode os.FileMode
}

// NewFileInfoFromAttrs
func NewFileInfoFromAttrs(obj minio.ObjectInfo, fileMode os.FileMode) *MinioFileInfo {
	res := &MinioFileInfo{
		ETag:     obj.ETag,
		name:     obj.Key,
		size:     obj.Size,
		updated:  obj.LastModified,
		isDir:    false,
		fileMode: fileMode,
	}

	if res.name == "" {
		// deals with them at the moment
		//res.name = "folder"
		res.size = folderSize
		res.isDir = true
	}

	return res
}

// Name
func (fi *MinioFileInfo) Name() string {
	return filepath.Base(filepath.FromSlash(fi.name))
}

// Size
func (fi *MinioFileInfo) Size() int64 {
	return fi.size
}

// Mode
func (fi *MinioFileInfo) Mode() os.FileMode {
	if fi.IsDir() {
		return os.ModeDir | fi.fileMode
	}
	return fi.fileMode
}

// ModTime
func (fi *MinioFileInfo) ModTime() time.Time {
	return fi.updated
}

// IsDir
func (fi *MinioFileInfo) IsDir() bool {
	return fi.isDir
}

// Sys
func (fi *MinioFileInfo) Sys() any {
	return nil
}

type ByName []*MinioFileInfo

// Len
func (a ByName) Len() int { return len(a) }

// Swap
func (a ByName) Swap(i, j int) {
	a[i].name, a[j].name = a[j].name, a[i].name
	a[i].size, a[j].size = a[j].size, a[i].size
	a[i].updated, a[j].updated = a[j].updated, a[i].updated
	a[i].isDir, a[j].isDir = a[j].isDir, a[i].isDir
}

// Less
func (a ByName) Less(i, j int) bool {
	return strings.Compare(a[i].Name(), a[j].Name()) == -1
}
