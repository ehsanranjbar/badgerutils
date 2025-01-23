package object

import (
	"encoding"
	"fmt"

	"github.com/araddon/qlbridge/expr"
	qlvm "github.com/araddon/qlbridge/vm"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/extutil"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/internal/qlutil"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/ehsanranjbar/badgerutils/schema"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

// Object is a generic object that can be stored in a Store.
type Object[
	I any,
	D encoding.BinaryMarshaler,
] struct {
	Id       *I             `json:"id,omitempty"`
	Data     D              `json:"data,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Store is a generic store for objects.
type Store[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
] struct {
	dataStore     *extstore.Store[D, PD]
	idFunc        func(*D) (I, error)
	idCodec       codec.Codec[I]
	metaStore     *extutil.AssociateStore[D, extutil.Metadata, *extutil.Metadata]
	indexers      map[string]*indexing.Extension[D]
	extractor     schema.PathExtractor[D]
	flatExtractor schema.PathExtractor[[]byte]
}

// New creates a new Store.
func New[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	base badgerutils.BadgerStore,
	opts ...func(*Store[I, D, PD]),
) (*Store[I, D, PD], error) {
	s := &Store[I, D, PD]{
		dataStore: extstore.New[D, PD](base),
		indexers:  map[string]*indexing.Extension[D]{},
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

	if s.metaStore == nil {
		s.metaStore = extutil.NewAssociateStore[D, extutil.Metadata]()
	}

	err := s.dataStore.AddExtension("meta_associate_store", s.metaStore)
	if err != nil {
		return nil, fmt.Errorf("failed to add meta_associate_store extension: %w", err)
	}

	for name, idx := range s.indexers {
		extName := "idx/" + name
		err := s.dataStore.AddExtension(extName, idx)
		if err != nil {
			return nil, fmt.Errorf("failed to add indexer extension %q: %w", name, err)
		}
	}

	if s.extractor == nil {
		s.extractor = schema.NewReflectPathExtractor[D](true)
	}

	return s, nil
}

// WithIdFunc is an option to set the id function.
func WithIdFunc[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	f func(*D) (I, error),
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.idFunc = f
	}
}

// WithIdCodec is an option to set the id codec.
func WithIdCodec[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	c codec.Codec[I],
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.idCodec = c
	}
}

// WithMetadataFunc is an option to set the metadata function.
func WithMetadataFunc[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	f func(_ []byte, _ *D, _ D, oldU, newU *extutil.Metadata) (*extutil.Metadata, error),
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.metaStore = extutil.NewAssociateStore(extutil.WithSynthFunc(f))
	}
}

// WithIndexer is an option to add an indexer.
func WithIndexer[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	name string,
	idx indexing.Indexer[D],
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		if _, ok := s.indexers[name]; ok {
			panic("indexer already exists")
		}

		s.indexers[name] = indexing.NewExtension(idx)
	}
}

// WithExtractor is an option to set the extractor.
func WithExtractor[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	e schema.PathExtractor[D],
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.extractor = e
	}
}

// WithFlatExtractor is an option to set the flat extractor.
func WithFlatExtractor[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	e schema.PathExtractor[[]byte],
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.flatExtractor = e
	}
}

// Prefix returns the prefix of the store.
func (s *Store[I, D, PD]) Prefix() []byte {
	return s.dataStore.Prefix()
}

// Delete deletes the key from the store.
func (s *Store[I, D, PD]) Delete(id I) error {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return err
	}

	return s.dataStore.Delete(key)
}

// Get gets the object with given id from the store.
func (s *Store[I, D, PD]) Get(id I) (*D, error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	return s.dataStore.Get(key)
}

// GetObject gets the object with given id from the store.
func (s *Store[I, D, PD]) GetObject(id I) (*Object[I, D], error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	d, err := s.dataStore.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object's data: %w", err)
	}
	obj := &Object[I, D]{Id: &id, Data: *d}
	meta, err := s.metaStore.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object's metadata: %w", err)
	}
	if meta != nil {
		obj.Metadata = *meta
	}

	return obj, nil
}

// NewIterator creates a new iterator over the objects.
func (s *Store[I, D, PD]) NewIterator(opts badger.IteratorOptions) *Iterator[I, D] {
	return newIterator(s.dataStore.NewIterator(opts), s.idCodec, s.metaStore, true)
}

// Set sets the object with given id to the store.
func (s *Store[I, D, PD]) Set(d D, opts ...func(*Object[I, D])) error {
	obj := &Object[I, D]{Data: d}
	for _, opt := range opts {
		opt(obj)
	}

	if obj.Id == nil {
		if s.idFunc == nil {
			return fmt.Errorf("no id function with nil id")
		}

		id, err := s.idFunc(&d)
		if err != nil {
			return err
		}
		obj.Id = &id
	}

	return s.SetObject(obj)
}

// WithId is an option to set the id of the object.
func WithId[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	id I,
) func(*Object[I, D]) {
	return func(o *Object[I, D]) {
		o.Id = &id
	}
}

// WithMetadata is an option to set the metadata of the object.
func WithMetadata[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	m map[string]any,
) func(*Object[I, D]) {
	return func(o *Object[I, D]) {
		o.Metadata = m
	}
}

// SetObject sets the object to the store.
func (s *Store[I, D, PD]) SetObject(obj *Object[I, D]) error {
	if obj.Id == nil {
		return fmt.Errorf("nil id is not allowed")
	}

	key, err := s.idCodec.Encode(*obj.Id)
	if err != nil {
		return err
	}

	return s.dataStore.SetWithOptions(key, &obj.Data, extutil.WithAssociateData(extutil.Metadata(obj.Metadata)))
}

// AddIndexer adds an indexer to the store.
func (s *Store[I, D, PD]) AddIndexer(name string, idx indexing.Indexer[D]) error {
	if _, ok := s.indexers[name]; ok {
		return fmt.Errorf("indexer %q already exists", name)
	}

	idxExt := indexing.NewExtension(idx)
	extName := "idx/" + name
	err := s.dataStore.AddExtension(extName, idxExt)
	if err != nil {
		return fmt.Errorf("failed to add indexer extension %q: %w", name, err)
	}

	s.indexers[name] = idxExt
	return nil
}

// Indexer returns the indexer with given name.
func (s *Store[I, D, PD]) Indexer(name string) *indexing.Extension[D] {
	idx, ok := s.indexers[name]
	if !ok {
		return nil
	}

	return idx
}

// Query returns the query for the store.
func (s *Store[I, D, PD]) Query(q string) (badgerutils.Iterator[*Object[I, D]], error) {
	qe, err := expr.ParseExpression(q)
	if err != nil {
		return nil, err
	}

	iter := iters.Filter(
		s.NewIterator(badger.DefaultIteratorOptions),
		func(obj *Object[I, D], item *badger.Item) bool {
			ctx := qlutil.NewObjectContextWrapper(obj.Id, obj.Data, obj.Metadata, s.extractor, nil)
			t, _ := qlvm.MatchesExpr(ctx, qe)
			return t
		})
	return iter, nil
}
