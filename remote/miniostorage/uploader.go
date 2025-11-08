package miniostorage

import (
	"io"

	"github.com/tenrok/filestore/remote"
)

func (s *MinioStorage) Uploader() remote.Uploader { return s }

func (s *MinioStorage) Upload(path string, reader io.Reader, opts ...remote.Option) error {
	file, err := s.Create(path, opts...)
	if err != nil {
		return err
	}

	if _, err := io.Copy(file, reader); err != nil {
		file.Close()
		return err
	}

	return file.Close()
}
