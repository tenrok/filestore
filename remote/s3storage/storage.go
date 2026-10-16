package s3storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/tenrok/filestore/remote"
)

// Проверка на этапе компиляции, что тип реализует remote.Storage.
var _ remote.Storage = (*S3Storage)(nil)

func init() {
	remote.Register("s3", &S3Storage{})
	// "minio" — устаревший алиас для обратной совместимости.
	// Работает через тот же S3Storage, т.к. MinIO полностью S3-совместим.
	remote.Register("minio", &S3Storage{})
}

type S3Storage struct {
	ctx      context.Context
	client   *s3.Client
	uploader *manager.Uploader
	cfg      *Config
}

// NewStorage создаёт S3-хранилище по строке подключения.
// Поддерживает scheme "s3" и "minio" (deprecated).
func (s *S3Storage) NewStorage(ctx context.Context, connString string) (remote.Storage, error) {
	cfg, err := NewConfig(connString)
	if err != nil {
		return nil, err
	}

	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	// Статические креды используются только если они заданы в строке
	// подключения. Иначе остаётся default chain (env, ~/.aws, IAM role).
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		optFns = append(optFns, config.WithCredentialsProvider(
			// Token (session token) передаётся третьим аргументом —
			// обратная совместимость с minio://?token=...
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.Token),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(endpointURL(cfg.Endpoint, cfg.Secure))
		}
		// Path-style работает и для MinIO, и для Ceph, и для AWS.
		o.UsePathStyle = true
		// Отключаем логирование пропущенных проверок
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	return &S3Storage{
		ctx:      ctx,
		client:   client,
		uploader: manager.NewUploader(client),
		cfg:      cfg,
	}, nil
}

// Create возвращает стриминговый writer.
func (s *S3Storage) Create(name string, opts ...remote.Option) (io.WriteCloser, error) {
	key := joinPrefix(s.cfg.Prefix, name)
	return newS3Writer(s.ctx, s.uploader, s.cfg.Bucket, key, opts...), nil
}

// Open возвращает http.File. Отсутствующий объект маппится в os.ErrNotExist.
func (s *S3Storage) Open(name string) (http.File, error) {
	key := joinPrefix(s.cfg.Prefix, name)

	head, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	return newS3FileWrapper(
		s.ctx, s.client, s.cfg.Bucket, key, name,
		aws.ToInt64(head.ContentLength),
		aws.ToTime(head.LastModified),
	), nil
}

// Remove удаляет объект. Отсутствие ключа не считается ошибкой (поведение S3).
func (s *S3Storage) Remove(name string) error {
	key := joinPrefix(s.cfg.Prefix, name)
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// Stat возвращает метаданные объекта. Отсутствие — os.ErrNotExist.
func (s *S3Storage) Stat(name string) (remote.FileInfo, error) {
	key := joinPrefix(s.cfg.Prefix, name)
	info, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return &s3FileInfo{
		key:          name,
		size:         aws.ToInt64(info.ContentLength),
		lastModified: aws.ToTime(info.LastModified),
	}, nil
}

// IsExists возвращает (false, nil) при отсутствии объекта.
func (s *S3Storage) IsExists(name string) (bool, error) {
	key := joinPrefix(s.cfg.Prefix, name)
	_, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List возвращает страницу объектов. Имена — относительно префикса хранилища.
// Это гарантирует, что FileInfo.Name(), переданный в Open/Stat/Remove,
// указывает на тот же объект.
func (s *S3Storage) List(ctx context.Context, prefix string, limit int, token string) (*remote.ListResult, error) {
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}

	sp := storagePrefix(s.cfg.Prefix)
	s3Prefix := joinPrefix(sp, prefix)
	// Гарантируем слэш на конце, чтобы "foo" не матчил "foobar".
	if s3Prefix != "" && !strings.HasSuffix(s3Prefix, "/") {
		s3Prefix += "/"
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.cfg.Bucket),
		MaxKeys: aws.Int32(int32(limit)),
	}
	if s3Prefix != "" {
		input.Prefix = aws.String(s3Prefix)
	}
	if token != "" {
		input.ContinuationToken = aws.String(token)
	}

	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, err
	}

	files := make([]remote.FileInfo, 0, len(output.Contents))
	for _, obj := range output.Contents {
		fullKey := aws.ToString(obj.Key)
		if sp != "" && !strings.HasPrefix(fullKey, sp) {
			continue // safety: чужой namespace
		}
		relKey := strings.TrimPrefix(fullKey, sp)
		files = append(files, &s3FileInfo{
			key:          relKey,
			size:         aws.ToInt64(obj.Size),
			lastModified: aws.ToTime(obj.LastModified),
		})
	}

	nextToken := ""
	if aws.ToBool(output.IsTruncated) && output.NextContinuationToken != nil {
		nextToken = aws.ToString(output.NextContinuationToken)
	}
	return &remote.ListResult{Files: files, NextToken: nextToken}, nil
}

// isNotFound распознаёт ошибки S3, означающие «объект не найден».
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket", "404":
			return true
		}
	}
	return false
}
