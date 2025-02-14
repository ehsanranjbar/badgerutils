package object

import (
	"bytes"
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
	msgpack "github.com/vmihailenco/msgpack/v5"
)

var _ badgerutils.StoreInstance[int64, *struct{}, struct{}, *Iterator[int64, struct{}]] = (*Instance[int64, struct{}])(nil)

// Object is a generic object that can be stored in a Store.
type Object[I, D any] struct {
	Id       *I             `json:"id,omitempty"`
	Data     D              `json:"data,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (obj Object[I, D]) MarshalBinary() ([]byte, error) {
	enc := msgpack.GetEncoder()
	var buf bytes.Buffer
	enc.Reset(&buf)
	defer msgpack.PutEncoder(enc)

	err := enc.Encode(&obj.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object's data: %w", err)
	}

	err = enc.EncodeMap(obj.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object's metadata: %w", err)
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (obj *Object[I, D]) UnmarshalBinary(data []byte) error {
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	defer msgpack.PutDecoder(dec)

	err := dec.Decode(&obj.Data)
	if err != nil {
		return fmt.Errorf("failed to decode object's data: %w", err)
	}

	obj.Metadata, err = dec.DecodeMap()
	if err != nil {
		return fmt.Errorf("failed to decode object's metadata: %w", err)
	}

	return nil
}

// Store is a generic store for objects.
type Store[I, D any] struct {
	base          *extstore.Store[Object[I, D], *Object[I, D]]
	idFunc        func(*D) (I, error)
	idCodec       codec.Codec[I]
	metadataFunc  func(key []byte, oldV *D, newV D, oldU, newU *map[string]any) (*map[string]any, error)
	indexers      map[string]*indexing.Extension[D]
	extractor     schema.PathExtractor[D]
	flatExtractor schema.PathExtractor[[]byte]
}

// New creates a new Store.
func New[I, D any](
	base badgerutils.Instantiator[badgerutils.BadgerStore],
	opts ...func(*Store[I, D]),
) (*Store[I, D], error) {
	s := &Store[I, D]{
		indexers: map[string]*indexing.Extension[D]{},
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

	if s.metadataFunc == nil {
		s.metadataFunc = extutil.MetadataSynthFunc[D, map[string]any](true)
	}

	exts := map[string]extstore.Extension[Object[I, D]]{}
	for name, idx := range s.indexers {
		extName := "idx/" + name
		exts[extName] = extutil.NewMapWrapper(idx, func(obj *Object[I, D]) *D {
			return &obj.Data
		})
	}
	s.base = extstore.New[Object[I, D], *Object[I, D]](base, exts)

	if s.extractor == nil {
		s.extractor = schema.NewReflectPathExtractor[D](true)
	}

	return s, nil
}

// WithIdFunc is an option to set the id function.
func WithIdFunc[I, D any](
	f func(*D) (I, error),
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		s.idFunc = f
	}
}

// WithIdCodec is an option to set the id codec.
func WithIdCodec[I, D any](
	c codec.Codec[I],
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		s.idCodec = c
	}
}

// WithMetadataFunc is an option to set the metadata function.
func WithMetadataFunc[I, D any](
	f func(_ []byte, _ *D, _ D, oldU, newU *map[string]any) (*map[string]any, error),
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		s.metadataFunc = f
	}
}

// WithIndexer is an option to add an indexer.
func WithIndexer[I, D any](
	name string,
	idx indexing.Indexer[D],
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		if _, ok := s.indexers[name]; ok {
			panic("indexer already exists")
		}

		s.indexers[name] = indexing.NewExtension(idx).(*indexing.Extension[D])
	}
}

// WithExtractor is an option to set the extractor.
func WithExtractor[I, D any](
	e schema.PathExtractor[D],
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		s.extractor = e
	}
}

// WithFlatExtractor is an option to set the flat extractor.
func WithFlatExtractor[I, D any](
	e schema.PathExtractor[[]byte],
) func(*Store[I, D]) {
	return func(s *Store[I, D]) {
		s.flatExtractor = e
	}
}

// Prefix returns the prefix of the store.
func (s *Store[I, D]) Prefix() []byte {
	return s.base.Prefix()
}

// Instantiate implements the badgerutils.Instantiator interface.
func (s *Store[I, D]) Instantiate(txn *badger.Txn) *Instance[I, D] {
	return &Instance[I, D]{
		base:         s.base.Instantiate(txn),
		idFunc:       s.idFunc,
		idCodec:      s.idCodec,
		metadataFunc: s.metadataFunc,
		extractor:    s.extractor,
	}
}

// Indexer returns the indexer with given name.
func (s *Store[I, D]) Indexer(name string) *indexing.Extension[D] {
	idx, ok := s.indexers[name]
	if !ok {
		return nil
	}

	return idx
}

// Instance is an instance of the Store.
type Instance[I, D any] struct {
	base         *extstore.Instance[Object[I, D], *Object[I, D]]
	idFunc       func(*D) (I, error)
	idCodec      codec.Codec[I]
	metadataFunc func(key []byte, oldV *D, newV D, oldU, newU *map[string]any) (*map[string]any, error)
	extractor    schema.PathExtractor[D]
}

// Delete deletes the key from the store.
func (s *Instance[I, D]) Delete(id I) error {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return err
	}

	return s.base.Delete(key)
}

// Get implements the badgerutils.StoreInstance interface.
func (s *Instance[I, D]) Get(id I) (*D, error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	obj, err := s.base.Get(key)
	if err != nil {
		return nil, err
	}
	return &obj.Data, nil
}

// GetObject gets the object with given id from the store.
func (s *Instance[I, D]) GetObject(id I) (*Object[I, D], error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	obj, err := s.base.Get(key)
	if err != nil {
		return nil, err
	}
	obj.Id = &id

	return obj, nil
}

// NewIterator creates a new iterator over the objects.
func (s *Instance[I, D]) NewIterator(opts badger.IteratorOptions) *Iterator[I, D] {
	return newIterator(s.base.NewIterator(opts), s.idCodec)
}

// Set implements the badgerutils.StoreInstance interface.
func (s *Instance[I, D]) Set(key I, data D) error {
	obj := &Object[I, D]{
		Id:   &key,
		Data: data,
	}
	return s.SetObject(obj)
}

// SetObject sets the object to the store.
func (s *Instance[I, D]) SetObject(obj *Object[I, D], opts ...any) error {
	if obj.Id == nil {
		if s.idFunc == nil {
			return fmt.Errorf("no id function with nil id")
		}

		id, err := s.idFunc(&obj.Data)
		if err != nil {
			return err
		}
		obj.Id = &id
	}

	key, err := s.idCodec.Encode(*obj.Id)
	if err != nil {
		return err
	}

	err = s.updateMetadata(key, obj)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	return s.base.SetWithOptions(key, obj, opts...)
}

func (s *Instance[I, D]) updateMetadata(key []byte, obj *Object[I, D]) error {
	oldObj, err := s.base.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	var (
		oldData *D
		oldMeta *map[string]any
	)
	if oldObj != nil {
		oldData = &oldObj.Data
		oldMeta = &oldObj.Metadata
	}
	newMeta, err := s.metadataFunc(key, oldData, obj.Data, oldMeta, &obj.Metadata)
	if err != nil {
		return fmt.Errorf("failed to synthesize metadata: %w", err)
	}
	obj.Metadata = *newMeta

	return nil
}

// Query returns the query for the store.
func (s *Instance[I, D]) Query(q string) (badgerutils.Iterator[[]byte, *Object[I, D]], error) {
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
