package concat

import (
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	reflectutils "github.com/ehsanranjbar/badgerutils/utils/reflect"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
type Indexer[T any] struct {
	components []*verifiedComponent
	retriever  indexing.ValueRetriever[T]
}

// New creates a new indexer for the given struct type and components.
func New[T any](comps ...Component) (*Indexer[T], error) {
	rt := reflect.TypeFor[T]()
	if reflectutils.GetBaseType(rt).Kind() != reflect.Struct {
		return nil, fmt.Errorf("indexer only supports struct types but got %s", rt)
	}

	vcs, err := verifyComponents(rt, comps)
	if err != nil {
		return nil, err
	}

	return &Indexer[T]{components: vcs}, nil
}

func verifyComponents(t reflect.Type, comps []Component) ([]*verifiedComponent, error) {
	vcs := make([]*verifiedComponent, len(comps))
	for i, comp := range comps {
		vc, err := comp.verify(t)
		if err != nil {
			return nil, err
		}
		vcs[i] = vc
	}
	return vcs, nil
}

// SetRetriever sets the retriever for the indexer.
func (si *Indexer[T]) SetRetriever(retriever indexing.ValueRetriever[T]) {
	si.retriever = retriever
}

// Index implements the Indexer interface.
func (si *Indexer[T]) Index(v *T, set bool) ([]badgerutils.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	rf := reflect.ValueOf(v).Elem()
	keys, err := si.composeKeys(rf)
	if err != nil {
		return nil, err
	}

	value := []byte(nil)
	if si.retriever != nil && set {
		value = si.retriever.RetrieveValue(v)
	}

	pairs := make([]badgerutils.RawKVPair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, badgerutils.RawKVPair{Key: key, Value: value})
	}
	return pairs, nil
}

func (si *Indexer[T]) composeKeys(rf reflect.Value) ([][]byte, error) {
	var keys [][]byte
	for _, comp := range si.components {
		v, ok := reflectutils.SafeFieldByIndex(rf, comp.fieldIndex)
		if !ok {
			return nil, fmt.Errorf("failed to get field by index %v", comp.fieldIndex)
		}

		suffixes, err := comp.encode(v.Interface())
		if err != nil {
			return nil, err
		}

		if len(keys) == 0 {
			keys = suffixes
		} else {
			if len(suffixes) == 1 {
				for i, k := range keys {
					keys[i] = append(k, suffixes[0]...)
				}
			} else {
				keys = propagateKeys(keys, suffixes)
			}
		}
	}

	return keys, nil
}

func propagateKeys(keys [][]byte, suffixes [][]byte) [][]byte {
	newKeys := make([][]byte, 0, len(keys)*len(suffixes))
	for _, k := range keys {
		for _, s := range suffixes {
			newKeys = append(newKeys, append(k, s...))
		}
	}

	return newKeys
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

		l, h, err := comp.encodeBounds(e.Low(), e.High())
		if err != nil {
			return nil, err
		}

		low = append(low, l...)
		high = append(high, h...)
	}

	return iters.Slice([]indexing.Partition{indexing.NewPartition(
		indexing.NewBound(low, false),
		indexing.NewBound(high, false),
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

func (si *Indexer[T]) findComponent(path string) *verifiedComponent {
	for _, comp := range si.components {
		if comp.path == path {
			return comp
		}
	}
	return nil
}
