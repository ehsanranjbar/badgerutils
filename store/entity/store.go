package entity

import (
	"fmt"
	"sync"

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
	indexers      map[string]*indexing.Extension[T]
	extractor     schema.PathExtractor[*T]
	flatExtractor schema.PathExtractor[[]byte]
	initialized   bool
	init          sync.Once
}

// New creates a new Store.
func New[
	I comparable,
	T any,
	PT Entity[I, T],
](
	base badgerutils.Instantiator[badgerutils.BadgerStore],
) *Store[I, T, PT] {
	return &Store[I, T, PT]{
		base:      extstore.New[T, PT](base),
		idCodec:   codec.CodecFor[I](),
		indexers:  map[string]*indexing.Extension[T]{},
		extractor: schema.NewReflectPathExtractor[*T](true),
	}
}

// WithIdFunc is an option to set the id function.
func (s *Store[I, T, PT]) WithIdFunc(f func(*T) (I, error)) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.idFunc = f
	return s
}

// WithSequenceAsIdFunc is an option to set the id function to use a sequence.
func WithSequenceAsIdFunc[
	I constraints.Integer,
	T any,
](
	seq *badger.Sequence,
) func(_ *T) (I, error) {
	return func(_ *T) (I, error) {
		id, err := seq.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to generate id: %w", err)
		}
		// badger sequences start from 0, so we increment it by 1.
		return I(id + 1), nil
	}
}

// WithIdCodec is an option to set the id codec.
func (s *Store[I, T, PT]) WithIdCodec(c codec.Codec[I]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.idCodec = c
	return s
}

// WithExtractor is an option to set the extractor.
func (s *Store[I, T, PT]) WithExtractor(e schema.PathExtractor[*T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.extractor = e
	return s
}

// WithFlatExtractor is an option to set the flat extractor.
func (s *Store[I, T, PT]) WithFlatExtractor(e schema.PathExtractor[[]byte]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.flatExtractor = e
	return s
}

// WithIndexer adds an indexer to the store.
func (s *Store[I, T, PT]) WithIndexer(name string, idx indexing.Indexer[T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	if _, ok := s.indexers[name]; ok {
		panic("indexer already exists")
	}

	ext := indexing.NewExtension(idx).(*indexing.Extension[T])
	s.base.WithExtension(name, ext)
	s.indexers[name] = ext
	return s
}

// WithExtension adds an extension to the store.
func (s *Store[I, T, PT]) WithExtension(name string, ext extstore.Extension[T]) *Store[I, T, PT] {
	if s.initialized {
		panic("store already initialized")
	}

	s.base.WithExtension(name, ext)
	return s
}

// Prefix returns the prefix of the store.
func (s *Store[I, T, PT]) Prefix() []byte {
	return s.base.Prefix()
}

// Instantiate implements the badgerutils.Instantiator interface.
func (s *Store[I, T, PT]) Instantiate(txn *badger.Txn) *Instance[I, T, PT] {
	// Locking any changes to the store's configuration on first instantiation.
	s.init.Do(func() {
		if s.idCodec == nil {
			panic("no codec for id")
		}

		if s.extractor == nil {
			panic("no extractor")
		}

		s.initialized = true
	})

	return &Instance[I, T, PT]{
		base:      s.base.Instantiate(txn),
		idFunc:    s.idFunc,
		idCodec:   s.idCodec,
		extractor: s.extractor,
	}
}

// IdCodec returns the id codec.
func (s *Store[I, T, PT]) IdCodec() codec.Codec[I] {
	return s.idCodec
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
