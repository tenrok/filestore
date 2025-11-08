package miniostorage

import (
	"io/fs"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
)

// Убеждаемся в том, что мы всегда реализуем интерфейс fs.FileInfo.
var _ fs.FileInfo = (*minioFileInfo)(nil)

// minioFileInfo реализует fs.FileInfo.
type minioFileInfo struct {
	info minio.ObjectInfo
}

func newMinioFileInfo(info minio.ObjectInfo) *minioFileInfo {
	return &minioFileInfo{info: info}
}

func (f *minioFileInfo) Name() string { return f.info.Key }

func (f *minioFileInfo) Size() int64 { return f.info.Size }

func (f *minioFileInfo) Mode() os.FileMode { return 0644 } // MinIO не поддерживает права доступа к файлам. Возвращаем значение по умолчанию.

func (f *minioFileInfo) ModTime() time.Time { return f.info.LastModified.Local() }

func (f *minioFileInfo) IsDir() bool { return f.info.Key[len(f.info.Key)-1] == '/' }

func (f *minioFileInfo) Sys() interface{} { return f.info }
