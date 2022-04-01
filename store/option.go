package store

import "go.uber.org/zap/zapcore"

type EmptyValueEnabler interface {
	EnableEmpty()
}

type Option interface {
	Apply(s Store)
}

type emptyValueOpt struct {
}

func WithEmptyValue() Option {
	return emptyValueOpt{}
}

func (e emptyValueOpt) Apply(s Store) {
	if f, ok := s.(EmptyValueEnabler); ok {
		f.EnableEmpty()
	}
}

func NewReadOptions(opts ...ReadOption) (out *ReadOptions) {
	if len(opts) == 0 {
		return nil
	}

	out = &ReadOptions{}
	for _, opt := range opts {
		opt.Apply(out)
	}

	return out
}

type ReadOptions struct {
	KeyOnly bool
}

func (o *ReadOptions) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	if o == nil {
		encoder.AddBool("key_only", false)
		return nil
	}

	encoder.AddBool("key_only", o.KeyOnly)
	return nil
}

type ReadOption interface {
	Apply(o *ReadOptions)
}

func KeyOnly() ReadOption {
	return keyOnlyReadOption{}
}

type keyOnlyReadOption struct{}

func (o keyOnlyReadOption) Apply(opts *ReadOptions) {
	opts.KeyOnly = true
}
