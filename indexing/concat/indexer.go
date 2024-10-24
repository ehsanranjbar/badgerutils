package concat

import (
	"bytes"
	"fmt"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/codec/lex"
	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/ehsanranjbar/badgerutils/iters"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
type Indexer[T any] struct {
	components []Component
	extractor  codec.PathExtractor[T, []lex.Value]
}

// New creates a new indexer for the given struct type and components.
func New[T any](
	extractor codec.PathExtractor[T, []lex.Value],
	comps ...Component,
) (*Indexer[T], error) {
	return &Indexer[T]{
		components: comps,
		extractor:  extractor,
	}, nil
}

// Index implements the Indexer interface.
func (si *Indexer[T]) Index(v *T, set bool) ([]badgerutils.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	keys, err := si.composeKeys(*v)
	if err != nil {
		return nil, err
	}

	pairs := make([]badgerutils.RawKVPair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, badgerutils.RawKVPair{Key: key, Value: nil})
	}
	return pairs, nil
}

func (si *Indexer[T]) composeKeys(v T) ([][]byte, error) {
	var keys [][]byte
	for _, comp := range si.components {
		values, err := si.extractor.ExtractPath(v, comp.path)
		if err != nil {
			return nil, fmt.Errorf("failed to extract path %s: %w", comp.path, err)
		}

		suffixes := make([][]byte, len(values))
		for i := 0; i < len(values); i++ {
			suffixes[i] = comp.postProcess(values[i])
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
			e = exprs.NewRange[lex.Value](nil, nil)
		}

		switch e := e.(type) {
		case exprs.Equal[lex.Value]:
			v := comp.postProcess(e.Value())

			pars = propagateRanges(pars, exprs.NewRange(exprs.NewBound(v, false), exprs.NewBound(v, false)))
		case exprs.Range[lex.Value]:
			r, err := si.encodeRange(comp, e)
			if err != nil {
				return nil, fmt.Errorf("failed to encode range for %s: %w", comp.path, err)
			}

			pars = propagateRanges(pars, r)
		case exprs.In[lex.Value]:
			ranges := make([]exprs.Range[[]byte], 0, len(e.Values()))
			for _, v := range e.Values() {
				bz := comp.postProcess(v)
				ranges = append(ranges, exprs.NewRange(exprs.NewBound(bz, false), exprs.NewBound(bz, false)))
			}

			pars = propagateRanges(pars, ranges...)
		default:
			return nil, fmt.Errorf("unsupported expression type %T", e)
		}
	}

	return iters.Slice(pars), nil
}

func (si *Indexer[T]) encodeRange(comp Component, r exprs.Range[lex.Value]) (exprs.Range[[]byte], error) {
	var low, high []byte
	if r.Low().IsEmpty() {
		if comp.descending {
			low = bytes.Repeat([]byte{0xff}, comp.size)
		} else {
			low = make([]byte, comp.size)
		}
	} else {
		low = comp.postProcess(r.Low().Value())
	}

	if r.High().IsEmpty() {
		if comp.descending {
			high = make([]byte, comp.size)
		} else {
			high = bytes.Repeat([]byte{0xff}, comp.size)
		}
	} else {
		high = comp.postProcess(r.High().Value())
	}

	return exprs.NewRange(exprs.NewBound(low, r.Low().Exclusive()), exprs.NewBound(high, r.High().Exclusive())), nil
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

func (si *Indexer[T]) findComponent(path string) *Component {
	for _, comp := range si.components {
		if comp.path == path {
			return &comp
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
