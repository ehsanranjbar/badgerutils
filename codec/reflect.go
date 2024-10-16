package codec

import (
	"fmt"
	"reflect"
	"strings"
)

// ReflectPathExtractor is a PathExtractor that uses reflection to extract the value.
type ReflectPathExtractor[T any] struct {
	rt    reflect.Type
	cache map[string][]int
}

// NewReflectPathExtractor creates a new ReflectPathExtractor for the given type.
func NewReflectPathExtractor[T any]() ReflectPathExtractor[T] {
	rt := reflect.TypeFor[T]()
	if rt.Kind() != reflect.Struct {
		panic(fmt.Sprintf("reflect path extractor only supports struct types but got %s", rt))
	}

	return ReflectPathExtractor[T]{
		rt:    rt,
		cache: make(map[string][]int),
	}
}

// ExtractPath implements the PathExtractor interface.
func (pe ReflectPathExtractor[T]) ExtractPath(v T, path string) (reflect.Value, error) {
	indices, ok := pe.cache[path]
	if !ok {
		var err error
		pe.cache[path], err = pe.verifyPath(path)
		if err != nil {
			return reflect.Value{}, err
		}
		indices = pe.cache[path]
	}

	rv := reflect.ValueOf(v)
	for _, i := range indices {
		var err error
		rv = unwrapPtrValue(rv).Field(i)
		if !rv.IsValid() {
			return reflect.Value{}, fmt.Errorf("invalid field %d: %w", i, err)
		}
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			return rv, nil
		}
	}

	return rv, nil
}

func (pe ReflectPathExtractor[T]) verifyPath(path string) ([]int, error) {
	indices := make([]int, 0)
	t := pe.rt
	for _, part := range strings.Split(path, ".") {
		f, ok := unwrapPtrType(t).FieldByName(part)
		if !ok {
			return nil, fmt.Errorf("field %s not found in %s", part, t)
		}

		indices = append(indices, f.Index...)
		t = f.Type
	}

	return indices, nil
}

func unwrapPtrType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}

func unwrapPtrValue(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	return v
}

// ReflectValueToAny converts the given reflect.Value to any.
func ReflectValueToAny(v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}

	return v.Interface(), nil
}
