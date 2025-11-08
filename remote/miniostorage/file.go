package miniostorage

import (
	"io/fs"
	"net/http"

	"github.com/minio/minio-go/v7"
)

// Убеждаемся в том, что мы всегда реализуем интерфейс http.File.
var _ http.File = (*minioFileWrapper)(nil)

// minioFileWrapper оборачивает minio.Object для реализации http.File.
type minioFileWrapper struct {
	*minio.Object
	name string
}

// Readdir требуется для http.File. Для файлов возвращает ошибку.
func (f *minioFileWrapper) Readdir(count int) ([]fs.FileInfo, error) {
	return nil, fs.ErrInvalid
}

// Stat возвращает FileInfo, требуемый для http.File.
func (f *minioFileWrapper) Stat() (fs.FileInfo, error) {
	info, err := f.Object.Stat()
	if err != nil {
		return nil, err
	}
	return &minioFileInfo{info: info}, nil
}
