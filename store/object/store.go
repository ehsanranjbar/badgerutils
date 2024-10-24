package object

import (
	"encoding"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/extensions"
	extstore "github.com/ehsanranjbar/badgerutils/store/extensible"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

// Object is a generic object that can be stored in a Store.
type Object[
	I any,
	D encoding.BinaryMarshaler,
] struct {
	ID       *I             `json:"id,omitempty"`
	Data     D              `json:"data,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Store is a generic store for objects.
type Store[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
] struct {
	dataStore *extstore.Store[D, PD]
	metaStore *extensions.AssociateStore[D, extensions.Metadata, *extensions.Metadata]
	idFunc    func(*D) (I, error)
	idCodec   codec.Codec[I]
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
		metaStore: extensions.NewAssociateStore[D, extensions.Metadata, *extensions.Metadata](),
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.idCodec == nil {
		s.idCodec = codec.CodecFor[I]()
		if s.idCodec == nil {
			panic("no codec for ID")
		}
	}

	err := s.dataStore.AddExtension("meta_associate_store", s.metaStore)
	if err != nil {
		return nil, fmt.Errorf("failed to add meta_associate_store extension: %w", err)
	}

	return s, nil
}

// WithIDFunc is an option to set the ID function.
func WithIDFunc[
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

// WithIDCodec is an option to set the ID codec.
func WithIDCodec[
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
	f func(_ []byte, _ *D, _ D, oldU, newU *extensions.Metadata) (*extensions.Metadata, error),
) func(*Store[I, D, PD]) {
	return func(s *Store[I, D, PD]) {
		s.metaStore = extensions.NewAssociateStore[D, extensions.Metadata, *extensions.Metadata](extensions.WithSynthFunc(f))
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
	obj := &Object[I, D]{ID: &id, Data: *d}
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

	if obj.ID == nil {
		if s.idFunc == nil {
			return fmt.Errorf("no ID function with nil ID")
		}

		id, err := s.idFunc(&d)
		if err != nil {
			return err
		}
		obj.ID = &id
	}

	return s.SetObject(obj)
}

// WithID is an option to set the ID of the object.
func WithID[
	I any,
	D encoding.BinaryMarshaler,
	PD sstore.PointerBinaryUnmarshaler[D],
](
	id I,
) func(*Object[I, D]) {
	return func(o *Object[I, D]) {
		o.ID = &id
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
	if obj.ID == nil {
		return fmt.Errorf("no ID with nil ID")
	}

	key, err := s.idCodec.Encode(*obj.ID)
	if err != nil {
		return err
	}

	return s.dataStore.SetWithOptions(key, &obj.Data, extensions.WithAssociateData(extensions.Metadata(obj.Metadata)))
}
