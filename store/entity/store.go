package entity

import (
	"fmt"

	"github.com/araddon/qlbridge/expr"
	qlvm "github.com/araddon/qlbridge/vm"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/internal/qlutil"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/ehsanranjbar/badgerutils/schema"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"golang.org/x/exp/constraints"
)

// Entity is a model for something that is serializable and has an unique id assigned to it.
type Entity[I comparable, T any] interface {
	sstore.PBS[T]
	GetId() I
	SetId(I)
}

// Store is a generic store for entities.
type Store[
	I comparable,
	T any,
	PT Entity[I, T],
] struct {
	base          *extstore.Store[T, PT]
	idFunc        func(*T) (I, error)
	idCodec       codec.Codec[I]
	exts          map[string]extstore.Extension[T]
	indexers      map[string]*indexing.Extension[T]
	extractor     schema.PathExtractor[*T]
	flatExtractor schema.PathExtractor[[]byte]
}

// New creates a new Store.
func New[
	I comparable,
	T any,
	PT Entity[I, T],
](
	base badgerutils.Instantiator[badgerutils.BadgerStore],
	opts ...func(*Store[I, T, PT]),
) (*Store[I, T, PT], error) {
	s := &Store[I, T, PT]{
		indexers: map[string]*indexing.Extension[T]{},
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.idCodec == nil {
		s.idCodec = codec.CodecFor[I]()
		if s.idCodec == nil {
			panic("no codec for id")
		}
	}

	extOpts := []func(*extstore.Store[T, PT]) error{}
	for name, ext := range s.exts {
		extOpts = append(
			extOpts,
			extstore.WithExtension[T, PT](name, ext),
		)
	}
	for name, idx := range s.indexers {
		extName := "idx/" + name
		extOpts = append(
			extOpts,
			extstore.WithExtension[T, PT](extName, idx),
		)
	}
	s.base = extstore.New(base, extOpts...)

	if s.extractor == nil {
		s.extractor = schema.NewReflectPathExtractor[*T](true)
	}

	return s, nil
}

// WithIdFunc is an option to set the id function.
func WithIdFunc[
	I comparable,
	T any,
	PT Entity[I, T],
](
	f func(*T) (I, error),
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		s.idFunc = f
	}
}

// WithSeqAsIdFunc is an option to set the id function to use a sequence.
func WithSeqAsIdFunc[
	I constraints.Integer,
	T any,
	PT Entity[I, T],
](
	seq *badger.Sequence,
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		s.idFunc = func(_ *T) (I, error) {
			id, err := seq.Next()
			if err != nil {
				return 0, fmt.Errorf("failed to generate id: %w", err)
			}
			// badger sequences start from 0, so we increment it by 1.
			return I(id + 1), nil
		}
	}
}

// WithIdCodec is an option to set the id codec.
func WithIdCodec[
	I comparable,
	T any,
	PT Entity[I, T],
](
	c codec.Codec[I],
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		s.idCodec = c
	}
}

// WithIndexer is an option to add an indexer.
func WithIndexer[
	I comparable,
	T any,
	PT Entity[I, T],
](
	name string,
	idx indexing.Indexer[T],
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		if _, ok := s.indexers[name]; ok {
			panic("indexer already exists")
		}

		s.indexers[name] = indexing.NewExtension(idx).(*indexing.Extension[T])
	}
}

// WithExtension is an option to add an extension.
func WithExtension[
	I comparable,
	T any,
	PT Entity[I, T],
](
	name string,
	ext *indexing.Extension[T],
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		if _, ok := s.exts[name]; ok {
			panic("extension already exists")
		}

		s.exts[name] = ext
	}
}

// WithExtractor is an option to set the extractor.
func WithExtractor[
	I comparable,
	T any,
	PT Entity[I, T],
](
	e schema.PathExtractor[*T],
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		s.extractor = e
	}
}

// WithFlatExtractor is an option to set the flat extractor.
func WithFlatExtractor[
	I comparable,
	T any,
	PT Entity[I, T],
](
	e schema.PathExtractor[[]byte],
) func(*Store[I, T, PT]) {
	return func(s *Store[I, T, PT]) {
		s.flatExtractor = e
	}
}

// Prefix returns the prefix of the store.
func (s *Store[I, T, PT]) Prefix() []byte {
	return s.base.Prefix()
}

// Instantiate implements the badgerutils.Instantiator interface.
func (s *Store[I, T, PT]) Instantiate(txn *badger.Txn) *Instance[I, T, PT] {
	return &Instance[I, T, PT]{
		base:      s.base.Instantiate(txn),
		idFunc:    s.idFunc,
		idCodec:   s.idCodec,
		extractor: s.extractor,
	}
}

// Indexer returns the indexer with given name.
func (s *Store[I, T, PT]) Indexer(name string) *indexing.Extension[T] {
	idx, ok := s.indexers[name]
	if !ok {
		return nil
	}

	return idx
}

// Instance is an instance of the Store.
type Instance[
	I comparable,
	T any,
	PT Entity[I, T],
] struct {
	base      *extstore.Instance[T, PT]
	idFunc    func(*T) (I, error)
	idCodec   codec.Codec[I]
	extractor schema.PathExtractor[*T]
}

// Delete implements the badgerutils.StoreInstance interface.
func (s *Instance[I, T, PT]) Delete(id I) error {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return err
	}

	return s.base.Delete(key)
}

// Get implements the badgerutils.StoreInstance interface.
func (s *Instance[I, T, PT]) Get(id I) (*T, error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	e, err := s.base.Get(key)
	if err != nil {
		return nil, err
	}
	PT(e).SetId(id)
	return e, nil
}

// NewIterator implements the badgerutils.StoreInstance interface.
func (s *Instance[I, T, PT]) NewIterator(opts badger.IteratorOptions) *Iterator[I, T] {
	return newIterator(s.base.NewIterator(opts), s.idCodec)
}

// Set implements the badgerutils.StoreInstance interface.
func (s *Instance[I, T, PT]) Set(v *T, opts ...any) error {
	var zero I
	if PT(v).GetId() == zero {
		if s.idFunc == nil {
			return fmt.Errorf("zero id with no id func")
		}

		id, err := s.idFunc(v)
		if err != nil {
			return fmt.Errorf("failed to get id: %w", err)
		}

		PT(v).SetId(id)
	}

	key, err := s.idCodec.Encode(PT(v).GetId())
	if err != nil {
		return fmt.Errorf("failed to encode id: %w", err)
	}

	return s.base.SetWithOptions(key, v, opts...)
}

// Query returns the query for the store.
func (s *Instance[I, T, PT]) Query(q string) (badgerutils.Iterator[I, *T], error) {
	qe, err := expr.ParseExpression(q)
	if err != nil {
		return nil, err
	}

	iter := iters.Filter(
		s.NewIterator(badger.DefaultIteratorOptions),
		func(e *T, item *badger.Item) bool {
			ctx := qlutil.NewContextWrapper(PT(e).GetId(), e, s.extractor, nil)
			t, _ := qlvm.MatchesExpr(ctx, qe)
			return t
		})
	return iter, nil
}
