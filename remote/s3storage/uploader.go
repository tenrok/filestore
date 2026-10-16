package s3storage

import (
	"io"

	"github.com/tenrok/filestore/remote"
)

func (s *S3Storage) Uploader() remote.Uploader { return s }

// Upload стримит reader в хранилище через Create + Copy + Close.
func (s *S3Storage) Upload(path string, reader io.Reader, opts ...remote.Option) error {
	file, err := s.Create(path, opts...)
	if err != nil {
		return err
	}

	if _, err := io.Copy(file, reader); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}
