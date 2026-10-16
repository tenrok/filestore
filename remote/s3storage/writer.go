package s3storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/tenrok/filestore/remote"
)

// Проверка на этапе компиляции, что тип реализует io.WriteCloser.
var _ io.WriteCloser = (*s3Writer)(nil)

// writeBufferSize — размер буфера, накапливаемого до отправки в pipe.
const writeBufferSize = 256 * 1024 // 256 KiB

// s3Writer реализует io.WriteCloser поверх manager.Uploader.
// Данные стримятся в S3 без полной буферизации в памяти.
type s3Writer struct {
	ctx context.Context
	pr  *io.PipeReader
	pw  *io.PipeWriter
	bw  *bufio.Writer

	done      chan error
	closeOnce sync.Once
	closeErr  error
}

// newS3Writer запускает фоновую загрузку и возвращает writer.
// Сетевой запрос инициируется сразу (multipart), но данные отправляются по мере поступления.
func newS3Writer(ctx context.Context, uploader *manager.Uploader, bucket, key string, opts ...remote.Option) *s3Writer {
	o := &remote.Options{}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	pr, pw := io.Pipe()

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr,
	}
	if o.ContentType != "" {
		input.ContentType = aws.String(o.ContentType)
	}
	if len(o.Metadata) > 0 {
		meta := make(map[string]string, len(o.Metadata))
		for k, v := range o.Metadata {
			// S3 требует корректные имена метаданных; отбрасываем пустые ключи.
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			meta[k] = fmt.Sprintf("%v", v)
		}
		if len(meta) > 0 {
			input.Metadata = meta
		}
	}

	w := &s3Writer{
		ctx:  ctx,
		pr:   pr,
		pw:   pw,
		bw:   bufio.NewWriterSize(pw, writeBufferSize),
		done: make(chan error, 1),
	}

	go func() {
		_, err := uploader.Upload(ctx, input)
		// CloseWithError гарантирует, что pending Write не зависнет,
		// если uploader завершился из-за ошибки.
		_ = pr.CloseWithError(err)
		w.done <- err
	}()

	return w
}

func (w *s3Writer) Write(p []byte) (int, error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.bw.Write(p)
	}
}

// Close идемпотентен: повторные вызовы возвращают ту же ошибку, что и первый.
func (w *s3Writer) Close() error {
	w.closeOnce.Do(func() {
		if err := w.bw.Flush(); err != nil {
			w.closeErr = err
		}
		if err := w.pw.Close(); err != nil && w.closeErr == nil {
			w.closeErr = err
		}
		if err := <-w.done; err != nil && w.closeErr == nil {
			w.closeErr = err
		}
	})
	return w.closeErr
}
