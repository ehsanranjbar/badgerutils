package indexing

import (
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils"
	reflectutils "github.com/ehsanranjbar/badgerutils/utils/reflect"
)

// Indexer is a wrapper around an Indexer that injects custom values to the indexes.
type ValueInjector[T any] struct {
	indexer   Indexer[T]
	retriever ValueRetriever[T]
}

func NewValueInjector[T any](
	indexer Indexer[T],
	retriever ValueRetriever[T],
) *ValueInjector[T] {
	if indexer == nil {
		panic("indexer is required")
	}
	if retriever == nil {
		panic("retriever is required")
	}

	return &ValueInjector[T]{
		indexer:   indexer,
		retriever: retriever,
	}
}

// Index implements the Indexer interface.
func (i *ValueInjector[T]) Index(v *T, set bool) ([]badgerutils.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	idxs, err := i.indexer.Index(v, set)
	if err != nil {
		return nil, err
	}

	if set {
		value, err := i.retriever.RetrieveValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve value: %v", err)
		}

		for i := range idxs {
			idxs[i].Value = value
		}
	}

	return idxs, nil
}

// Lookup implements the Indexer interface.
func (i *ValueInjector[T]) Lookup(args ...any) (badgerutils.Iterator[Partition], error) {
	return i.indexer.Lookup(args...)
}

// ValueRetriever is an interface that retrieves index custom values for index only scans from the indexed type.
type ValueRetriever[T any] interface {
	RetrieveValue(v *T) ([]byte, error)
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
) *MapValueRetriever[T] {
	if encoder == nil {
		panic("encoder is required")
	}

	var t T
	reflectType := reflect.TypeOf(t)
	if reflectutils.GetBaseType(reflectType).Kind() != reflect.Struct {
		panic("map value retriever only supports struct types")
	}

	fields, err := extractFields(reflectType, paths)
	if err != nil {
		return nil
	}

	return &MapValueRetriever[T]{
		encoder: encoder,
		fields:  fields,
	}
}

func extractFields(t reflect.Type, paths []string) (map[string][]int, error) {
	fields := make(map[string][]int)
	for _, path := range paths {
		_, index, err := reflectutils.ExtractPath(t, path)
		if err != nil {
			return nil, err
		}
		fields[path] = index
	}
	return fields, nil
}

// RetrieveValue implements the ValueRetriever interface.
func (r *MapValueRetriever[T]) RetrieveValue(v *T) ([]byte, error) {
	m := map[string]any{}
	if v != nil {
		for path, fi := range r.fields {
			f, ok := reflectutils.SafeFieldByIndex(reflect.ValueOf(v).Elem(), fi)
			if ok {
				m[path] = f.Interface()
			} else {
				m[path] = nil
			}
		}
	}
	b, err := r.encoder(m)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %v", err)
	}
	return b, nil
}
