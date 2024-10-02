package indexing

import (
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils/utils"
)

// ValueRetriever is an interface that retrieves index custom values for index only scans from the indexed type.
type ValueRetriever[T any] interface {
	RetrieveValue(v *T) []byte
}

// MapValueRetriever is a value retriever that retrieves the given field paths of struct and encodes them to bytes.
type MapValueRetriever[T any] struct {
	fields  map[string][]int
	encoder func(v any) ([]byte, error)
}

// NewMapValueRetriever creates a new map value retriever for the given struct type and field paths.
func NewMapValueRetriever[T any](
	encoder func(v any) ([]byte, error),
	paths ...string,
) (*MapValueRetriever[T], error) {
	if encoder == nil {
		return nil, fmt.Errorf("encoder is required")
	}

	var t T
	reflectType := reflect.TypeOf(t)
	if utils.GetBaseType(reflectType).Kind() != reflect.Struct {
		return nil, fmt.Errorf("map value retriever only supports struct types but got %T", t)
	}

	fields, err := extractFields(reflectType, paths)
	if err != nil {
		return nil, err
	}

	return &MapValueRetriever[T]{
		encoder: encoder,
		fields:  fields,
	}, nil
}

func extractFields(t reflect.Type, paths []string) (map[string][]int, error) {
	fields := make(map[string][]int)
	for _, path := range paths {
		_, index, err := utils.ExtractPath(t, path)
		if err != nil {
			return nil, err
		}
		fields[path] = index
	}
	return fields, nil
}

// RetrieveValue implements the ValueRetriever interface.
func (r *MapValueRetriever[T]) RetrieveValue(v *T) []byte {
	m := map[string]any{}
	if v != nil {
		for path, fi := range r.fields {
			f, ok := utils.SafeFieldByIndex(reflect.ValueOf(v).Elem(), fi)
			if ok {
				m[path] = f.Interface()
			} else {
				m[path] = nil
			}
		}
	}
	b, err := r.encoder(m)
	if err != nil {
		panic(fmt.Sprintf("failed to encode value: %v", err))
	}
	return b
}
