package indexing

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
)

// TagIndexer is an indexer that indexes an struct type by the tags of its fields.
// The tag key "index" is used to specify the index name.
type TagIndexer[T any] struct {
	indexes map[string]tagIndex
}

// NewTagIndexer creates a new TagIndexer.
func NewTagIndexer[T any]() (*TagIndexer[T], error) {
	var t T
	reflectType := reflect.TypeOf(t)
	if reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}
	if reflectType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("tag indexer only supports struct types")
	}

	indexes, err := extractIndexes(reflectType)
	if err != nil {
		return nil, err
	}

	return &TagIndexer[T]{indexes: indexes}, nil
}

func extractIndexes(t reflect.Type) (map[string]tagIndex, error) {
	indexes := make(map[string]tagIndex)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag, ok := field.Tag.Lookup("index")
		if !ok {
			continue
		}

		name, index, err := parseTagIndex(field, tag)
		if err != nil {
			return nil, err
		}

		indexes[name] = index
	}

	return indexes, nil
}

func parseTagIndex(field reflect.StructField, tag string) (string, tagIndex, error) {
	kind := field.Type.Kind()
	if kind != reflect.String && kind != reflect.Int && kind != reflect.Int64 && kind != reflect.Uint && kind != reflect.Uint64 {
		return "", tagIndex{}, fmt.Errorf("unsupported field type %s", field.Type)
	}

	idx := tagIndex{field: field.Index[0], kind: kind}
	if tag == "" {
		return field.Name, idx, nil
	}

	return tag, idx, nil
}

type tagIndex struct {
	field int
	kind  reflect.Kind
}

// Index implements the Indexer interface.
func (i *TagIndexer[T]) Index(v T, _ bool) []badgerutils.RawKVPair {
	result := make([]badgerutils.RawKVPair, 0, len(i.indexes))
	for name, index := range i.indexes {
		value := reflect.ValueOf(v).Field(index.field)
		var key []byte
		switch index.kind {
		case reflect.String:
			key = []byte(value.String())
		case reflect.Int, reflect.Int64:
			key = []byte(binary.LittleEndian.AppendUint64(nil, uint64(value.Int())))
		case reflect.Uint, reflect.Uint64:
			key = []byte(binary.LittleEndian.AppendUint64(nil, value.Uint()))
		default:
			panic("unsupported kind")
		}

		result = append(result, badgerutils.RawKVPair{
			Key:   append([]byte(name), key...),
			Value: nil,
		})
	}

	return result
}

// Lookup implements the Indexer interface.
func (i *TagIndexer[T]) Lookup(args ...any) (badgerutils.Iterator[Partition], error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("at least one argument is required")
	}

	name, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string argument")
	}
	index, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf("index %s not found", name)
	}

	key := []byte(name)
	if len(args) > 1 {
		value := args[1]
		switch index.kind {
		case reflect.String:
			key = append(key, []byte(value.(string))...)
		case reflect.Int, reflect.Int64:
			key = append(key, binary.LittleEndian.AppendUint64(nil, uint64(value.(int64)))...)
		case reflect.Uint, reflect.Uint64:
			key = append(key, binary.LittleEndian.AppendUint64(nil, value.(uint64))...)
		default:
			return nil, fmt.Errorf("unsupported kind")
		}
	}

	return iters.Slice([]Partition{NewPrefixPartition(key)}), nil
}
