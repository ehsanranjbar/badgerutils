package concat

import (
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/ehsanranjbar/badgerutils/iters"
	reflectutils "github.com/ehsanranjbar/badgerutils/utils/reflect"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
type Indexer[T any] struct {
	components []*verifiedComponent
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

	pairs := make([]badgerutils.RawKVPair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, badgerutils.RawKVPair{Key: key, Value: nil})
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
func (si *Indexer[T]) Lookup(args ...any) (badgerutils.Iterator[exprs.Range[[]byte]], error) {
	exs, err := si.verifyExprs(args)
	if err != nil {
		return nil, fmt.Errorf("invalid lookup arguments: %w", err)
	}

	var pars []exprs.Range[[]byte]
	for _, comp := range si.components {
		e, ok := exs[comp.path]
		if !ok {
			e = exprs.NewRange[any](nil, nil)
		}

		switch e := e.(type) {
		case exprs.Equal:
			v, err := comp.encodeValue(e.Value())
			if err != nil {
				return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
			}

			pars = propagateRanges(pars, exprs.NewRange(exprs.NewBound(v, false), exprs.NewBound(v, false)))
		case exprs.Range[any]:
			r, err := comp.encodeRange(e)
			if err != nil {
				return nil, fmt.Errorf("failed to encode range for %s: %w", comp.path, err)
			}

			pars = propagateRanges(pars, r)
		case exprs.In:
			ranges := make([]exprs.Range[[]byte], 0, len(e.Values()))
			for _, v := range e.Values() {
				bz, err := comp.encodeValue(v)
				if err != nil {
					return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
				}

				ranges = append(ranges, exprs.NewRange(exprs.NewBound(bz, false), exprs.NewBound(bz, false)))
			}

			pars = propagateRanges(pars, ranges...)
		default:
			return nil, fmt.Errorf("unsupported expression type %T", e)
		}
	}

	return iters.Slice(pars), nil
}

func (si *Indexer[T]) verifyExprs(args []any) (map[string]any, error) {
	if len(args) > len(si.components) {
		return nil, fmt.Errorf("too many arguments %d, expected %d", len(args), len(si.components))
	}

	exs := make(map[string]any)
	for _, arg := range args {
		e, ok := arg.(exprs.Named)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type %T", arg)
		}

		if si.findComponent(e.Name()) == nil {
			return nil, fmt.Errorf("unsupported path %s", e.Name())
		}

		if old, ok := exs[e.Name()]; ok {
			return nil, fmt.Errorf("duplicate path %s in %v and %v", e.Name(), old, e)
		}
		exs[e.Name()] = e.Expression()
	}
	return exs, nil
}

func (si *Indexer[T]) findComponent(path string) *verifiedComponent {
	for _, comp := range si.components {
		if comp.path == path {
			return comp
		}
	}
	return nil
}

func propagateRanges(pars []exprs.Range[[]byte], elems ...exprs.Range[[]byte]) []exprs.Range[[]byte] {
	if len(pars) == 0 {
		return elems
	}

	var newPars []exprs.Range[[]byte]
	for _, p := range pars {
		for _, e := range elems {
			newPars = append(newPars, appendRange(p, e))
		}
	}
	return newPars
}

func appendRange(p1 exprs.Range[[]byte], p2 exprs.Range[[]byte]) exprs.Range[[]byte] {
	return exprs.NewRange(
		exprs.NewBound(
			append(p1.Low().Value(), p2.Low().Value()...),
			p1.Low().Exclusive() || p2.Low().Exclusive(),
		),
		exprs.NewBound(
			append(p1.High().Value(), p2.High().Value()...),
			p1.High().Exclusive() || p2.High().Exclusive(),
		),
	)
}
