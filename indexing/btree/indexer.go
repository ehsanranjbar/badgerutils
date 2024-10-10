package btree

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
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
		size:        be.GetSizeByType(field.Type),
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
func (si *Indexer[T]) Index(v *T, set bool) []badgerutils.RawKVPair {
	if v == nil {
		return nil
	}

	rf := reflect.ValueOf(v).Elem()
	key := make([]byte, 0)
	for _, comp := range si.components {
		fv, ok := utils.SafeFieldByIndex(rf, comp.fieldIndex)
		if !ok {
			continue
		}
		compKey := comp.encoderFunc(fv.Interface())
		if comp.descending {
			compKey = be.InverseBytes(compKey)
		}
		key = append(key, compKey...)
	}

	value := []byte(nil)
	if si.retriever != nil && set {
		value = si.retriever.RetrieveValue(v)
	}
	return []badgerutils.RawKVPair{{Key: key, Value: value}}
}

// Lookup implements the Indexer interface.
func (si *Indexer[T]) Lookup(args ...any) (badgerutils.Iterator[indexing.Partition], error) {
	exprs, err := si.extractExprs(args)
	if err != nil {
		return nil, fmt.Errorf("invalid lookup arguments: %w", err)
	}

	var low, high []byte
	for _, comp := range si.components {
		e, ok := exprs[comp.path]
		if !ok {
			e = indexing.NewRangeLookupExpr(comp.path, indexing.EmptyBound[any](), indexing.EmptyBound[any]())
		}

		l, h, err := comp.calculateBounds(e)
		if err != nil {
			return nil, err
		}

		low = append(low, l...)
		high = append(high, h...)
	}

	return iters.Slice([]indexing.Partition{indexing.NewPartition(
		indexing.NewBound(low, false),
		indexing.NewBound(high, true),
	)}), nil
}

func (si *Indexer[T]) extractExprs(args []any) (map[string]indexing.RangeLookupExpr, error) {
	if len(args) > len(si.components) {
		return nil, fmt.Errorf("too many arguments %d, expected %d", len(args), len(si.components))
	}

	exprs := make(map[string]indexing.RangeLookupExpr)
	for _, arg := range args {
		e, ok := arg.(indexing.RangeLookupExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type %T", arg)
		}

		if si.findComponent(e.Path()) == nil {
			return nil, fmt.Errorf("unsupported path %s", e.Path())
		}

		if old, ok := exprs[e.Path()]; ok {
			return nil, fmt.Errorf("duplicate path %s in %v and %v", e.Path(), old, e)
		}
		exprs[e.Path()] = e
	}
	return exprs, nil
}

func (si *Indexer[T]) findComponent(path string) *structIndexComponent {
	for _, comp := range si.components {
		if comp.path == path {
			return &comp
		}
	}
	return nil
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
	size        int
}

func (comp structIndexComponent) calculateBounds(e indexing.RangeLookupExpr) ([]byte, []byte, error) {
	var low []byte
	if e.Low().IsEmpty() {
		if comp.size < 1 {
			return nil, nil, indexing.ErrUndefinedLookup
		}

		low = make([]byte, comp.size)
	} else {
		low = comp.encoderFunc(e.Low().Value())
		if comp.descending {
			low = be.InverseBytes(low)
		}
	}

	var high []byte
	if e.High().IsEmpty() {
		if comp.size < 1 {
			return nil, nil, indexing.ErrUndefinedLookup
		}

		high = bytes.Repeat([]byte{0xff}, comp.size)
	} else {
		high = comp.encoderFunc(e.High().Value())
		if comp.descending {
			high = be.InverseBytes(high)
		}
	}

	return low, high, nil
}

func (comp structIndexComponent) sized() bool {
	return comp.size > 0
}
