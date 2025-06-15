package miniostorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"sort"
	"syscall"

	"github.com/minio/minio-go/v7"
	"github.com/tenrok/filestore/remote"
)

var (
	ErrOutOfRange   = errors.New("out of range")
	ErrNotSupported = errors.New("doesn't support this operation")
)

var _ remote.File = (*MinioFile)(nil)

type MinioFile struct {
	openFlags int
	offset    int64
	closed    bool
	resource  *minioFileResource
}

// NewMinioFile
func NewMinioFile(ctx context.Context, storage *MinioStorage, openFlags int, fileMode os.FileMode, name string) *MinioFile {
	return &MinioFile{
		openFlags: openFlags,
		// offset:    0,
		// closed:    false,
		resource: &minioFileResource{
			ctx:           ctx,
			storage:       storage,
			name:          name,
			fileMode:      fileMode,
			currentIoSize: 0,
			offset:        0,
			reader:        nil,
			writer:        nil,
		},
	}
}

// Close
func (f *MinioFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return f.resource.Close()
}

// Read
func (f *MinioFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	readed, err := f.resource.ReadAt(p, f.offset)
	f.offset += int64(readed)
	return readed, err
}

// Seek
func (f *MinioFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	// Since this is an expensive operation; let's make sure we need it
	if (whence == 0 && offset == f.offset) || (whence == 1 && offset == 0) {
		return f.offset, nil
	}
	// Fore the reader/writers to be reopened (at correct offset)
	if err := f.Sync(); err != nil {
		return 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = stat.Size() + offset
	}
	return f.offset, nil
}

// Write
func (f *MinioFile) Write(p []byte) (int, error) {
	return f.WriteAt(p, f.offset)
}

// WriteAt
func (f *MinioFile) WriteAt(b []byte, off int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.openFlags&os.O_RDONLY != 0 {
		return 0, fmt.Errorf("file is opened as read only")
	}
	written, err := f.resource.WriteAt(b, off)
	f.offset += int64(written)
	return written, err
}

// readdirImpl
func (f *MinioFile) readdirImpl(count int) ([]*MinioFileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, err
	}
	ownInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !ownInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}
	var res []*MinioFileInfo
	objs := f.resource.storage.client.ListObjects(f.resource.ctx, f.resource.storage.bucket, minio.ListObjectsOptions{
		Recursive: true,
		Prefix:    f.resource.name,
	})
	for obj := range objs {
		tmp := NewFileInfoFromAttrs(obj, f.resource.fileMode)
		if tmp.Name() == "" {
			// neither object.Name, not object.Prefix were present - so let's skip this unknown thing
			continue
		}
		res = append(res, tmp)
	}
	if count > 0 && len(res) > 0 {
		sort.Sort(ByName(res))
		res = res[:count]
	}
	return res, nil
}

// Readdir
func (f *MinioFile) Readdir(count int) ([]fs.FileInfo, error) {
	fi, err := f.readdirImpl(count)
	if err != nil {
		return nil, err
	}
	var res []fs.FileInfo
	for _, v := range fi {
		res = append(res, v)
	}
	return res, nil
}

// Readdirnames
func (f *MinioFile) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil && err != io.EOF {
		return nil, err
	}
	names := make([]string, len(fi))
	for i, v := range fi {
		names[i] = v.Name()
	}
	return names, err
}

// Stat
func (f *MinioFile) Stat() (os.FileInfo, error) {
	if err := f.Sync(); err != nil {
		return nil, err
	}
	stat, err := f.resource.storage.client.StatObject(f.resource.ctx, f.resource.storage.bucket, f.resource.name, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return NewFileInfoFromAttrs(stat, f.resource.fileMode), nil
}

// Sync
func (f *MinioFile) Sync() error {
	return f.resource.maybeCloseIo()
}

// Truncate
func (f *MinioFile) Truncate(_ int64) error {
	return ErrNotSupported
}

// WriteString
func (f *MinioFile) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

type readerAtCloser interface {
	io.ReadCloser
	io.ReaderAt
}

type minioFileResource struct {
	ctx           context.Context
	storage       *MinioStorage
	name          string
	fileMode      os.FileMode
	currentIoSize int64
	offset        int64
	reader        readerAtCloser
	writer        io.WriteCloser
	closed        bool
}

// Close
func (r *minioFileResource) Close() error {
	r.closed = true
	return r.maybeCloseIo()
}

// maybeCloseIo
func (r *minioFileResource) maybeCloseIo() error {
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return fmt.Errorf("error closing reader: %v", err)
		}
		r.reader = nil
	}
	if r.writer != nil {
		if err := r.writer.Close(); err != nil {
			return fmt.Errorf("error closing writer: %v", err)
		}
		r.writer = nil
	}
	return nil
}

// ReadAt
func (r *minioFileResource) ReadAt(p []byte, offset int64) (int, error) {
	if cap(p) == 0 {
		return 0, nil
	}
	// Assume that if the reader is open; it is at the correct offset a good performance assumption that we must ensure holds
	if offset == r.offset && r.reader != nil {
		readed, err := r.reader.ReadAt(p, offset)
		r.offset += int64(readed)
		return readed, err
	}
	// If any writers have written anything; commit it first so we can read it back.
	if err := r.maybeCloseIo(); err != nil {
		return 0, err
	}
	obj, err := r.storage.client.GetObject(r.ctx, r.storage.bucket, r.name, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	r.reader = obj
	r.offset = offset
	readed, err := obj.ReadAt(p, offset)
	r.offset += int64(readed)
	return readed, err
}

// WriteAt
func (r *minioFileResource) WriteAt(b []byte, offset int64) (int, error) {
	// If the writer is opened and at the correct offset we're good!
	if offset == r.offset && r.writer != nil {
		written, err := r.writer.Write(b)
		r.offset += int64(written)
		return written, err
	}
	// Ensure readers must be re-opened and that if a writer is active at another offset it is first committed before we do a "seek" below
	if err := r.maybeCloseIo(); err != nil {
		return 0, err
	}
	// WriteAt to a non existing file
	if offset > r.currentIoSize {
		return 0, ErrOutOfRange
	}
	r.offset = offset
	buffer := bytes.NewReader(b)
	opts := minio.PutObjectOptions{
		ContentType: http.DetectContentType(b),
	}
	if offset > 0 {
		opts.PartSize = uint64(offset)
		opts.NumThreads = 8
		opts.ConcurrentStreamParts = false
		opts.DisableMultipart = true
	}
	if _, err := r.storage.client.PutObject(r.ctx, r.storage.bucket, r.name, buffer, buffer.Size(), opts); err != nil {
		return 0, err
	}
	r.offset += int64(buffer.Len())
	return buffer.Len(), nil
}
