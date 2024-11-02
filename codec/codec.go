package codec

import (
	"encoding"
	"reflect"

	"github.com/ehsanranjbar/badgerutils/codec/lex"
)

// Codec is an interface for encoding and decoding values.
type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}

// Encoder is an interface for encoding values.
type Encoder[T any] interface {
	Encode(v T) ([]byte, error)
}

// Decoder is an interface for decoding values.
type Decoder[T any] interface {
	Decode(bz []byte) (T, error)
}

// CodecFor returns the codec for the given type.
func CodecFor[T any]() Codec[T] {
	rt := reflect.TypeFor[T]()
	switch rt.Kind() {
	case reflect.String:
		return any(stringCodec{}).(Codec[T])
	case reflect.Int:
		return any(intCodec{}).(Codec[T])
	case reflect.Int64:
		return any(int64Codec{}).(Codec[T])
	case reflect.Uint:
		return any(uintCodec{}).(Codec[T])
	case reflect.Uint64:
		return any(uint64Codec{}).(Codec[T])
	case reflect.Array, reflect.Slice:
		if rt.Elem().Kind() == reflect.Uint8 {
			return any(bytesCodec{}).(Codec[T])
		}
	}

	if rt.Implements(reflect.TypeFor[encoding.BinaryMarshaler]()) &&
		reflect.TypeFor[*T]().Implements(reflect.TypeFor[encoding.BinaryUnmarshaler]()) {
		return binaryCodec[T]{}
	}

	return nil
}

type stringCodec struct{}

// Encode implements the Codec interface.
func (stringCodec) Encode(v string) ([]byte, error) {
	return []byte(v), nil
}

// Decode implements the Codec interface.
func (stringCodec) Decode(bz []byte) (string, error) {
	return string(bz), nil
}

type intCodec struct{}

// Encode implements the Codec interface.
func (intCodec) Encode(v int) ([]byte, error) {
	return lex.EncodeInt64(int64(v)), nil
}

// Decode implements the Codec interface.
func (intCodec) Decode(bz []byte) (int, error) {
	return int(lex.DecodeInt64(bz)), nil
}

type int64Codec struct{}

// Encode implements the Codec interface.
func (int64Codec) Encode(v int64) ([]byte, error) {
	return lex.EncodeInt64(v), nil
}

// Decode implements the Codec interface.
func (int64Codec) Decode(bz []byte) (int64, error) {
	return lex.DecodeInt64(bz), nil
}

type uintCodec struct{}

// Encode implements the Codec interface.
func (uintCodec) Encode(v uint) ([]byte, error) {
	return lex.EncodeUint64(uint64(v)), nil
}

// Decode implements the Codec interface.
func (uintCodec) Decode(bz []byte) (uint, error) {
	return uint(lex.DecodeUint64(bz)), nil
}

type uint64Codec struct{}

// Encode implements the Codec interface.
func (uint64Codec) Encode(v uint64) ([]byte, error) {
	return lex.EncodeUint64(v), nil
}

// Decode implements the Codec interface.
func (uint64Codec) Decode(bz []byte) (uint64, error) {
	return uint64(lex.DecodeUint64(bz)), nil
}

type bytesCodec struct{}

// Encode implements the Codec interface.
func (bytesCodec) Encode(v []byte) ([]byte, error) {
	return v, nil
}

// Decode implements the Codec interface.
func (bytesCodec) Decode(bz []byte) ([]byte, error) {
	return bz, nil
}

type binaryCodec[T any] struct{}

// Encode implements the Codec interface.
func (binaryCodec[T]) Encode(v T) ([]byte, error) {
	return any(v).(encoding.BinaryMarshaler).MarshalBinary()
}

// Decode implements the Codec interface.
func (binaryCodec[T]) Decode(bz []byte) (T, error) {
	var v T
	err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz)
	return v, err
}
