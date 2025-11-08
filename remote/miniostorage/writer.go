package miniostorage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/tenrok/filestore/remote"
)

// Убеждаемся в том, что мы всегда реализуем интерфейс io.WriteCloser.
var _ io.WriteCloser = (*minioWriter)(nil)

// minioWriter реализует интерфейс io.WriteCloser.
type minioWriter struct {
	ctx         context.Context
	client      *minio.Client
	bucket      string
	path        string
	buffer      *bytes.Buffer
	metadata    remote.Metadata
	contentType string
}

func newMinioWriter(ctx context.Context, client *minio.Client, bucket, path string, opts ...remote.Option) *minioWriter {
	o := &remote.Options{}
	for _, opt := range opts {
		opt(o)
	}

	writer := &minioWriter{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		path:   path,
		buffer: bytes.NewBuffer(nil),
	}

	if o.ContentType != "" {
		writer.contentType = o.ContentType
	}
	if o.Metadata != nil {
		writer.metadata = o.Metadata
	}

	return writer
}

func (w *minioWriter) Write(p []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.buffer.Write(p)
	}
}

func (w *minioWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		opts := minio.PutObjectOptions{}

		if w.contentType != "" {
			opts.ContentType = w.contentType
		}

		if w.metadata != nil {
			userMetadata := make(map[string]string)
			for k, v := range w.metadata {
				userMetadata[k] = fmt.Sprintf("%v", v)
			}
			opts.UserMetadata = userMetadata
		}

		_, err := w.client.PutObject(
			w.ctx,
			w.bucket,
			w.path,
			bytes.NewReader(w.buffer.Bytes()),
			int64(w.buffer.Len()),
			opts,
		)
		return err
	}
}
