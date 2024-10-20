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
		return any(StringCodec{}).(Codec[T])
	case reflect.Int:
		return any(IntCodec{}).(Codec[T])
	case reflect.Int64:
		return any(Int64Codec{}).(Codec[T])
	case reflect.Uint:
		return any(UintCodec{}).(Codec[T])
	case reflect.Uint64:
		return any(Uint64Codec{}).(Codec[T])
	case reflect.Array, reflect.Slice:
		if rt.Elem().Kind() == reflect.Uint8 {
			return any(BytesCodec{}).(Codec[T])
		}
	}

	if rt.Implements(reflect.TypeFor[encoding.BinaryMarshaler]()) &&
		reflect.TypeFor[*T]().Implements(reflect.TypeFor[encoding.BinaryUnmarshaler]()) {
		return BinaryCodec[T]{}
	}

	return nil
}

// StringCodec is a codec for strings.
type StringCodec struct{}

// Encode encodes the given string to bytes.
func (StringCodec) Encode(v string) ([]byte, error) {
	return []byte(v), nil
}

// Decode decodes the given bytes to a string.
func (StringCodec) Decode(bz []byte) (string, error) {
	return string(bz), nil
}

// IntCodec is a codec for integers.
type IntCodec struct{}

// Encode encodes the given integer to bytes.
func (IntCodec) Encode(v int) ([]byte, error) {
	return lex.EncodeInt64(int64(v)), nil
}

// Decode decodes the given bytes to an integer.
func (IntCodec) Decode(bz []byte) (int, error) {
	return int(lex.DecodeInt64(bz)), nil
}

// Int64Codec is a codec for int64s.
type Int64Codec struct{}

// Encode encodes the given int64 to bytes.
func (Int64Codec) Encode(v int64) ([]byte, error) {
	return lex.EncodeInt64(v), nil
}

// Decode decodes the given bytes to an int64.
func (Int64Codec) Decode(bz []byte) (int64, error) {
	return lex.DecodeInt64(bz), nil
}

// UintCodec is a codec for unsigned integers.
type UintCodec struct{}

// Encode encodes the given unsigned integer to bytes.
func (UintCodec) Encode(v uint) ([]byte, error) {
	return lex.EncodeUint64(uint64(v)), nil
}

// Decode decodes the given bytes to an unsigned integer.
func (UintCodec) Decode(bz []byte) (uint, error) {
	return uint(lex.DecodeUint64(bz)), nil
}

// Uint64Codec is a codec for uint64s.
type Uint64Codec struct{}

// Encode encodes the given uint64 to bytes.
func (Uint64Codec) Encode(v uint64) ([]byte, error) {
	return lex.EncodeUint64(v), nil
}

// Decode decodes the given bytes to a uint64.
func (Uint64Codec) Decode(bz []byte) (uint64, error) {
	return uint64(lex.DecodeUint64(bz)), nil
}

// BytesCodec is a codec for byte slices.
type BytesCodec struct{}

// Encode encodes the given byte slice to bytes.
func (BytesCodec) Encode(v []byte) ([]byte, error) {
	return v, nil
}

// Decode decodes the given bytes to a byte slice.
func (BytesCodec) Decode(bz []byte) ([]byte, error) {
	return bz, nil
}

// BinaryCodec is a codec for types that implement encoding.BinaryMarshaler and encoding.BinaryUnmarshaler.
type BinaryCodec[T any] struct{}

// Encode encodes the given value to bytes.
func (BinaryCodec[T]) Encode(v T) ([]byte, error) {
	return any(v).(encoding.BinaryMarshaler).MarshalBinary()
}

// Decode decodes the given bytes to a value.
func (BinaryCodec[T]) Decode(bz []byte) (T, error) {
	var v T
	err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz)
	return v, err
}
