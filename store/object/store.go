package object

import (
	"bytes"
	"encoding"
	"fmt"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	extstore "github.com/ehsanranjbar/badgerutils/store/extensible"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/vmihailenco/msgpack/v5"
)

// Object is a generic object that can be stored in a Store.
type Object[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
] struct {
	id       *I
	data     D
	metadata map[string]any
}

// ID returns the ID of the object.
func (o Object[I, D, DT]) ID() *I {
	return o.id
}

// Data returns the data of the object.
func (o Object[I, D, DT]) Data() D {
	return o.data
}

// MarshalBinary implements encoding.BinaryMarshaler
func (o Object[I, D, DT]) MarshalBinary() ([]byte, error) {
	dataBz, err := o.data.MarshalBinary()
	if err != nil {
		return nil, err
	}

	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	var buf bytes.Buffer
	enc.Reset(&buf)

	err = enc.EncodeMulti(dataBz, o.metadata)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (o *Object[I, D, DT]) UnmarshalBinary(bz []byte) error {
	dec := msgpack.GetDecoder()
	defer msgpack.PutDecoder(dec)
	dec.Reset(bytes.NewReader(bz))

	var dataBz []byte
	err := dec.DecodeMulti(&dataBz, &o.metadata)
	if err != nil {
		return err
	}

	err = DT(&o.data).UnmarshalBinary(dataBz)
	if err != nil {
		return err
	}

	return nil
}

// Store is a generic store for objects.
type Store[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
] struct {
	base         *extstore.Store[Object[I, D, DT], *Object[I, D, DT]]
	idFunc       func(*D) (I, error)
	idCodec      codec.Codec[I]
	metadataFunc func(*D) (map[string]any, error)
}

// NewStore creates a new Store.
func NewStore[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	base badgerutils.BadgerStore,
	opts ...func(*Store[I, D, DT]),
) *Store[I, D, DT] {
	s := &Store[I, D, DT]{base: extstore.New[Object[I, D, DT]](base)}
	for _, opt := range opts {
		opt(s)
	}

	if s.idCodec == nil {
		s.idCodec = codec.CodecFor[I]()
		if s.idCodec == nil {
			panic("no codec for ID")
		}
	}

	return s
}

// WithIDFunc is an option to set the ID function.
func WithIDFunc[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	f func(*D) (I, error),
) func(*Store[I, D, DT]) {
	return func(s *Store[I, D, DT]) {
		s.idFunc = f
	}
}

// WithIDCodec is an option to set the ID codec.
func WithIDCodec[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	c codec.Codec[I],
) func(*Store[I, D, DT]) {
	return func(s *Store[I, D, DT]) {
		s.idCodec = c
	}
}

// WithMetadataFunc is an option to set the metadata function.
func WithMetadataFunc[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	f func(*D) (map[string]any, error),
) func(*Store[I, D, DT]) {
	return func(s *Store[I, D, DT]) {
		s.metadataFunc = f
	}
}

// Prefix returns the prefix of the store.
func (s *Store[I, D, DT]) Prefix() []byte {
	if pfx, ok := any(s.base).(prefixed); ok {
		return pfx.Prefix()
	}

	return nil
}

type prefixed interface {
	Prefix() []byte
}

// Delete deletes the key from the store.
func (s *Store[I, D, DT]) Delete(id I) error {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return err
	}

	return s.base.Delete(key)
}

// Get gets the object with given id from the store.
func (s *Store[I, D, DT]) Get(id I) (*D, error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	obj, err := s.base.Get(key)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, nil
	}

	d := obj.Data()
	return &d, nil
}

// GetObject gets the object with given id from the store.
func (s *Store[I, D, DT]) GetObject(id I) (*Object[I, D, DT], error) {
	key, err := s.idCodec.Encode(id)
	if err != nil {
		return nil, err
	}

	return s.base.Get(key)
}

// Set sets the object with given id to the store.
func (s *Store[I, D, DT]) Set(d D, opts ...func(*Object[I, D, DT])) error {
	obj := &Object[I, D, DT]{data: d}
	for _, opt := range opts {
		opt(obj)
	}

	if obj.id == nil {
		if s.idFunc == nil {
			return fmt.Errorf("no ID function with nil ID")
		}

		id, err := s.idFunc(&d)
		if err != nil {
			return err
		}
		obj.id = &id
	}

	if obj.metadata == nil {
		if s.metadataFunc != nil {
			m, err := s.metadataFunc(&d)
			if err != nil {
				return err
			}
			obj.metadata = m
		}
	}

	return s.SetObject(obj)
}

// WithID is an option to set the ID of the object.
func WithID[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	id I,
) func(*Object[I, D, DT]) {
	return func(o *Object[I, D, DT]) {
		o.id = &id
	}
}

// WithMetadata is an option to set the metadata of the object.
func WithMetadata[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
](
	m map[string]any,
) func(*Object[I, D, DT]) {
	return func(o *Object[I, D, DT]) {
		o.metadata = m
	}
}

// SetObject sets the object to the store.
func (s *Store[I, D, DT]) SetObject(obj *Object[I, D, DT]) error {
	if obj.id == nil {
		return fmt.Errorf("no ID with nil ID")
	}

	key, err := s.idCodec.Encode(*obj.id)
	if err != nil {
		return err
	}

	return s.base.Set(key, obj)
}
