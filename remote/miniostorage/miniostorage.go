package miniostorage

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/tenrok/filestore/remote"
)

const defaultFileMode = 0o755

var _ remote.Storage = (*MinioStorage)(nil)

func init() {
	remote.Register("minio", &MinioStorage{})
}

type MinioStorage struct {
	ctx       context.Context
	client    *minio.Client
	bucket    string
	separator string
}

// NewStorage
func (s *MinioStorage) NewStorage(ctx context.Context, connString string) (remote.Storage, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}
	queries := u.Query()
	username, password := getUserPassword(u)
	token := queries.Get("token")
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(username, password, token),
		Region: "us-east-1",
	}
	if queries.Has("secure") {
		secure, err := strconv.ParseBool(queries.Get("secure"))
		if err != nil {
			return nil, err
		}
		opts.Secure = secure
	}
	if queries.Has("region") {
		opts.Region = queries.Get("region")
	}
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}
	s.ctx = ctx
	s.client = client
	s.bucket = u.Path[1:]
	s.separator = "/"
	return s, nil
}

// normSeparators will normalize all "\\" and "/" to the provided separator
func (s *MinioStorage) normSeparators(str string) string {
	return strings.Replace(strings.Replace(str, "\\", s.separator, -1), "/", s.separator, -1)
}

// Create
func (s *MinioStorage) Create(name string) (http.File, error) {
	return s.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0)
}

// Open
func (s *MinioStorage) Open(name string) (http.File, error) {
	return s.OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile
func (s *MinioStorage) OpenFile(name string, flag int, fileMode os.FileMode) (http.File, error) {
	if flag&os.O_APPEND != 0 {
		return nil, errors.New("appending files will lead to trouble")
	}
	name = strings.TrimPrefix(s.normSeparators(name), s.separator)
	file := NewMinioFile(s.ctx, s, flag, fileMode, name)
	var err error
	if flag&os.O_CREATE != 0 {
		_, err = file.WriteString("")
	}
	return file, err
}

// Remove
func (s *MinioStorage) Remove(name string) error {
	name = strings.TrimPrefix(s.normSeparators(name), s.separator)
	return s.client.RemoveObject(s.ctx, s.bucket, name, minio.RemoveObjectOptions{GovernanceBypass: true})
}

// RemoveAll
func (s *MinioStorage) RemoveAll(path string) error {
	path = strings.TrimPrefix(s.normSeparators(path), s.separator)
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		opts := minio.ListObjectsOptions{Prefix: path, Recursive: true}
		for object := range s.client.ListObjects(s.ctx, s.bucket, opts) {
			if object.Err != nil {
				panic(object.Err)
			}
			objectsCh <- object
		}
	}()
	errorCh := s.client.RemoveObjects(s.ctx, s.bucket, objectsCh, minio.RemoveObjectsOptions{})
	for e := range errorCh {
		return errors.New("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
	}
	return nil
}

// Rename
func (s *MinioStorage) Rename(oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	oldName = strings.TrimPrefix(s.normSeparators(oldName), s.separator)
	newName = strings.TrimPrefix(s.normSeparators(newName), s.separator)
	src := minio.CopySrcOptions{
		Bucket: s.bucket,
		Object: oldName,
	}
	dst := minio.CopyDestOptions{
		Bucket: s.bucket,
		Object: newName,
	}
	if _, err := s.client.CopyObject(s.ctx, dst, src); err != nil {
		return err
	}
	return s.Remove(oldName)
}

// Stat
func (s *MinioStorage) Stat(name string) (os.FileInfo, error) {
	name = strings.TrimPrefix(s.normSeparators(name), s.separator)
	file := NewMinioFile(s.ctx, s, os.O_RDWR, defaultFileMode, name)
	return file.Stat()
}
