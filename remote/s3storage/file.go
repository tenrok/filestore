package s3storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Проверка на этапе компиляции, что тип реализует http.File.
var _ http.File = (*s3FileWrapper)(nil)

// defaultChunkSize — размер диапазона для одного range-запроса.
const defaultChunkSize int64 = 4 * 1024 * 1024 // 4 MiB

// s3FileWrapper реализует http.File поверх S3 с ленивой подгрузкой содержимого через range-запросы. Память ограничена одним чанком.
type s3FileWrapper struct {
	ctx       context.Context
	client    *s3.Client
	bucket    string
	objectKey string
	name      string

	size    int64
	modTime time.Time

	pos           int64
	body          io.ReadCloser
	bodyRemaining int64
	chunkSize     int64
}

// newS3FileWrapper создаёт обёртку. Сетевой запрос не делается — данные подгружаются лениво при первом Read.
func newS3FileWrapper(ctx context.Context, client *s3.Client, bucket, objectKey, name string, size int64, modTime time.Time) *s3FileWrapper {
	return &s3FileWrapper{
		ctx:       ctx,
		client:    client,
		bucket:    bucket,
		objectKey: objectKey,
		name:      name,
		size:      size,
		modTime:   modTime,
		chunkSize: defaultChunkSize,
	}
}

// Read реализует io.Reader. Читает из текущего чанка, при исчерпании — открывает следующий range-запрос.
func (f *s3FileWrapper) Read(p []byte) (int, error) {
	if f.pos >= f.size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	if f.body == nil || f.bodyRemaining == 0 {
		if err := f.openRange(f.pos); err != nil {
			return 0, err
		}
	}

	// Ограничиваем запрос оставшимся размером чанка.
	if int64(len(p)) > f.bodyRemaining {
		p = p[:f.bodyRemaining]
	}

	n, err := f.body.Read(p)
	f.pos += int64(n)
	f.bodyRemaining -= int64(n)

	if err != nil {
		// Тело чанка закрылось раньше ожидаемого — сбрасываем, чтобы следующий Read открыл новый диапазон.
		_ = f.body.Close()
		f.body = nil
		f.bodyRemaining = 0
		// Настоящий EOF — только если мы достигли конца файла.
		if err == io.EOF && f.pos < f.size {
			err = nil
		}
	}

	return n, err
}

// Seek реализует io.Seeker. Текущий чанк закрывается; следующий Read подгрузит данные с новой позиции.
func (f *s3FileWrapper) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = f.pos + offset
	case io.SeekEnd:
		newPos = f.size + offset
	default:
		return 0, fmt.Errorf("s3FileWrapper.Seek: invalid whence %d", whence)
	}
	if newPos < 0 {
		return 0, fmt.Errorf("s3FileWrapper.Seek: negative position %d", newPos)
	}

	if f.body != nil {
		_ = f.body.Close()
		f.body = nil
		f.bodyRemaining = 0
	}

	f.pos = newPos
	return f.pos, nil
}

// Close реализует io.Closer.
func (f *s3FileWrapper) Close() error {
	if f.body != nil {
		err := f.body.Close()
		f.body = nil
		f.bodyRemaining = 0
		return err
	}
	return nil
}

// Readdir — обязательный метод http.File. Для одиночного объекта не поддерживается.
func (f *s3FileWrapper) Readdir(int) ([]fs.FileInfo, error) {
	return nil, fs.ErrInvalid
}

// Stat — обязательный метод http.File.
func (f *s3FileWrapper) Stat() (fs.FileInfo, error) {
	return &s3FileInfo{
		key:          f.name,
		size:         f.size,
		lastModified: f.modTime,
	}, nil
}

// openRange открывает новый range-запрос начиная с позиции start.
func (f *s3FileWrapper) openRange(start int64) error {
	end := start + f.chunkSize - 1
	if end >= f.size {
		end = f.size - 1
	}
	if end < start {
		return io.EOF
	}

	out, err := f.client.GetObject(f.ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.objectKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return err
	}

	f.body = out.Body
	f.bodyRemaining = end - start + 1
	return nil
}
