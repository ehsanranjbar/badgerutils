package codec

// PathExtractor is an interface for extracting a value with the given path from a given value.
type PathExtractor[T, U any] interface {
	ExtractPath(t T, path string) (U, error)
}

// ConvertPathExtractor is a PathExtractor that converts the extracted value from base extractor to the target type using the given converter.
type ConvertPathExtractor[T, U, V any] struct {
	base PathExtractor[T, U]
	c    func(U) (V, error)
}

// NewConvertPathExtractor creates a new ConvertPathExtractor with the given base extractor and converter.
func NewConvertPathExtractor[T, U, V any](base PathExtractor[T, U], c func(U) (V, error)) ConvertPathExtractor[T, U, V] {
	return ConvertPathExtractor[T, U, V]{base: base, c: c}
}

// ExtractPath implements the PathExtractor interface.
func (pe ConvertPathExtractor[T, U, V]) ExtractPath(t T, path string) (v V, err error) {
	u, err := pe.base.ExtractPath(t, path)
	if err != nil {
		return v, err
	}

	return pe.c(u)
}
