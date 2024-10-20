package object

import (
	"bytes"
	"encoding"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/vmihailenco/msgpack/v5"
)

// Object is a generic object that can be stored in a Store.
type Object[
	I any,
	D encoding.BinaryMarshaler,
	DT sstore.PointerBinaryUnmarshaler[D],
] struct {
	id       I
	data     D
	metadata map[string]any
}

func (o Object[I, D, DT]) ID() I {
	return o.id
}

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
	base    badgerutils.BadgerStore
	idFunc  func(*D) (I, error)
	idCodec codec.Codec[I]
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
	s := &Store[I, D, DT]{base: base}
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
