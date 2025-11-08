package miniostorage

import (
	"context"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tenrok/filestore/remote"
)

// Убеждаемся в том, что мы всегда реализуем интерфейс remote.Storage.
var _ remote.Storage = (*MinioStorage)(nil)

func init() {
	remote.Register("minio", &MinioStorage{})
}

type MinioStorage struct {
	ctx    context.Context
	client *minio.Client
	cfg    *Config
}

func (s *MinioStorage) NewStorage(ctx context.Context, connString string) (remote.Storage, error) {
	cfg, err := NewConfig(connString)
	if err != nil {
		return nil, err
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretKey, cfg.Token),
		Region: cfg.Region,
		Secure: cfg.Secure,
	}

	client, err := minio.New(cfg.Endpoint, opts)
	if err != nil {
		return nil, err
	}

	s.ctx = ctx
	s.client = client
	s.cfg = cfg

	return s, nil
}

func (s *MinioStorage) Create(name string, opts ...remote.Option) (io.WriteCloser, error) {
	name = path.Join(s.cfg.Prefix, name)

	return newMinioWriter(s.ctx, s.client, s.cfg.BucketName, name, opts...), nil
}

func (s *MinioStorage) Open(name string) (http.File, error) {
	name = path.Join(s.cfg.Prefix, name)

	obj, err := s.client.GetObject(s.ctx, s.cfg.BucketName, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &minioFileWrapper{Object: obj, name: name}, nil
}

func (s *MinioStorage) Remove(name string) error {
	name = path.Join(s.cfg.Prefix, name)

	return s.client.RemoveObject(s.ctx, s.cfg.BucketName, name, minio.RemoveObjectOptions{})
}

func (s *MinioStorage) Stat(name string) (remote.FileInfo, error) {
	name = path.Join(s.cfg.Prefix, name)

	info, err := s.client.StatObject(s.ctx, s.cfg.BucketName, name, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return newMinioFileInfo(info), nil
}

func (s *MinioStorage) Exists(name string) (bool, error) {
	name = path.Join(s.cfg.Prefix, name)

	// Сначала проверяем, является ли путь файлом
	if ok, err := s.IsFile(name); err == nil && ok {
		return true, nil
	}
	// Если не файл, то проверяем, является ли путь каталогом
	return s.IsDir(name)
}

func (s *MinioStorage) IsDir(name string) (bool, error) {
	name = path.Join(s.cfg.Prefix, name)

	options := minio.ListObjectsOptions{
		Prefix:    strings.TrimRight(name, "/") + "/",
		Recursive: false,
		MaxKeys:   1,
	}
	objectChan := s.client.ListObjects(s.ctx, s.cfg.BucketName, options)
	object, ok := <-objectChan
	if !ok {
		return false, nil
	}
	if object.Err != nil {
		return false, object.Err
	}
	return true, nil
}

func (s *MinioStorage) IsFile(name string) (bool, error) {
	name = path.Join(s.cfg.Prefix, name)

	_, err := s.client.StatObject(s.ctx, s.cfg.BucketName, name, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if strings.Contains(err.Error(), "The specified key does not exist.") {
		return false, nil
	}
	return false, err
}
