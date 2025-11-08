package remote

type Option func(*Options)

type Options struct {
	Metadata    Metadata
	ContentType string
}

// WithMetadata устанавливает метаданные.
func WithMetadata(metadata Metadata) Option {
	return func(o *Options) {
		o.Metadata = metadata
	}
}

// WithContentType устанавливает тип файла.
func WithContentType(contentType string) Option {
	return func(o *Options) {
		o.ContentType = contentType
	}
}
