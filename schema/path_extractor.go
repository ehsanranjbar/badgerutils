package schema

import (
	"fmt"
	"reflect"
	"strings"
)

// PathExtractor is an interface for extracting a value with the given path from a given value.
type PathExtractor[T any] interface {
	ExtractPath(t T, path string) (any, error)
}

// ReflectPathExtractor is a PathExtractor that uses reflection to extract the value.
type ReflectPathExtractor[T any] struct {
	rt        reflect.Type
	returnAny bool
	cache     map[string][]int
}

// NewReflectPathExtractor creates a new ReflectPathExtractor for the given type.
func NewReflectPathExtractor[T any](returnAny bool) ReflectPathExtractor[T] {
	rt := reflect.TypeFor[T]()
	if rt.Kind() != reflect.Struct {
		panic(fmt.Sprintf("reflect path extractor only supports struct types but got %s", rt))
	}

	return ReflectPathExtractor[T]{
		rt:        rt,
		returnAny: returnAny,
		cache:     make(map[string][]int),
	}
}

// ExtractPath implements the PathExtractor interface.
func (pe ReflectPathExtractor[T]) ExtractPath(v T, path string) (any, error) {
	indices, ok := pe.cache[path]
	if !ok {
		var err error
		pe.cache[path], err = pe.verifyPath(path)
		if err != nil {
			return nil, err
		}
		indices = pe.cache[path]
	}

	rv := reflect.ValueOf(v)
	for _, i := range indices {
		var err error
		rv = unwrapPtr(rv).Field(i)
		if !rv.IsValid() {
			return nil, fmt.Errorf("invalid field %d: %w", i, err)
		}
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			return rv, nil
		}
	}

	if pe.returnAny {
		return rv.Interface(), nil
	} else {
		return rv, nil
	}
}

func (pe ReflectPathExtractor[T]) verifyPath(path string) ([]int, error) {
	indices := make([]int, 0)
	t := pe.rt
	for _, part := range strings.Split(path, ".") {
		f, ok := unwrapPtr(t).FieldByName(part)
		if !ok {
			return nil, fmt.Errorf("field %s not found in %s", part, t)
		}

		indices = append(indices, f.Index...)
		t = f.Type
	}

	return indices, nil
}

type reflectPtr[T any] interface {
	Kind() reflect.Kind
	Elem() T
}

func unwrapPtr[T reflectPtr[T]](v T) T {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	return v
}
