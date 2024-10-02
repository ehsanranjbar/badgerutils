package btree

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/utils"
	"github.com/ehsanranjbar/badgerutils/utils/be"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
type Indexer[T any] struct {
	components []structIndexComponent
	retriever  indexing.ValueRetriever[T]
}

// New creates a new indexer for the given struct type and components.
func New[T any](comps ...string) (*Indexer[T], error) {
	var t T
	reflectType := reflect.TypeOf(t)
	if utils.GetBaseType(reflectType).Kind() != reflect.Struct {
		return nil, fmt.Errorf("indexer only supports struct types but got %T", t)
	}

	components, err := extractComponents(reflectType, comps)
	if err != nil {
		return nil, err
	}

	return &Indexer[T]{components: components}, nil
}

func extractComponents(t reflect.Type, comps []string) ([]structIndexComponent, error) {
	components := make([]structIndexComponent, 0)
	for _, comp := range comps {
		compType, err := extractComponent(t, comp)
		if err != nil {
			return nil, err
		}
		components = append(components, compType)
	}
	return components, nil
}

func extractComponent(t reflect.Type, comp string) (structIndexComponent, error) {
	path, desc, err := parseComponent(comp)
	if err != nil {
		return structIndexComponent{}, err
	}

	field, index, err := utils.ExtractPath(t, path)
	if err != nil {
		return structIndexComponent{}, fmt.Errorf("invalid component path %s: %w", path, err)
	}

	return structIndexComponent{
		path:        path,
		fieldIndex:  index,
		kind:        field.Type.Kind(),
		encoderFunc: be.GetEncodeFuncByType(field.Type),
		descending:  desc,
	}, nil
}

func parseComponent(comp string) (string, bool, error) {
	parts := strings.Split(comp, " ")
	if len(parts) > 2 {
		return "", false, fmt.Errorf("invalid component %s", comp)
	}

	var desc bool
	if len(parts) > 1 {
		if strings.EqualFold(parts[1], "desc") {
			desc = true
		} else {
			return "", false, fmt.Errorf("invalid component %s", comp)
		}
	}

	return parts[0], desc, nil
}

// SetRetriever sets the retriever for the indexer.
func (si *Indexer[T]) SetRetriever(retriever indexing.ValueRetriever[T]) {
	si.retriever = retriever
}

// Index implements the Indexer interface.
func (si *Indexer[T]) Index(v *T, _ bool) []badgerutils.RawKVPair {
	if v == nil {
		return nil
	}

	key := make([]byte, 0)
	for _, comp := range si.components {
		compKey := comp.encoderFunc(reflect.ValueOf(v).Elem().FieldByIndex(comp.fieldIndex).Interface())
		if comp.descending {
			compKey = be.InverseBytes(compKey)
		}
		key = append(key, compKey...)
	}

	value := []byte(nil)
	if si.retriever != nil {
		value = si.retriever.RetrieveValue(v)
	}
	return []badgerutils.RawKVPair{{Key: key, Value: value}}
}

// String returns a string representation of the index.
func (si *Indexer[T]) String() string {
	var sb strings.Builder
	for i, comp := range si.components {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(comp.path)
		if comp.descending {
			sb.WriteString(" DESC")
		}
	}
	return sb.String()
}

type structIndexComponent struct {
	path        string
	fieldIndex  []int
	kind        reflect.Kind
	encoderFunc be.EncodeFunc
	descending  bool
}
