package s3storage

import (
	"os"
	"time"

	"github.com/tenrok/filestore/remote"
)

// Проверка на этапе компиляции, что тип реализует remote.FileInfo.
var _ remote.FileInfo = (*s3FileInfo)(nil)

// s3FileInfo реализует remote.FileInfo.
type s3FileInfo struct {
	key          string
	size         int64
	lastModified time.Time
}

func (f *s3FileInfo) Name() string {
	return f.key
}

func (f *s3FileInfo) Size() int64 {
	return f.size
}

// Mode: S3 не хранит POSIX-права. Возвращаем 0644 для файлов и 0755 для «каталогов».
func (f *s3FileInfo) Mode() os.FileMode {
	if f.IsDir() {
		return os.ModeDir | 0755
	}
	return 0644
}

func (f *s3FileInfo) ModTime() time.Time {
	return f.lastModified.Local()
}

// IsDir: объект считается «каталогом», если ключ заканчивается на '/'.
// Защищено от паники на пустом ключе.
func (f *s3FileInfo) IsDir() bool {
	return len(f.key) > 0 && f.key[len(f.key)-1] == '/'
}

func (f *s3FileInfo) Sys() any {
	return nil
}
