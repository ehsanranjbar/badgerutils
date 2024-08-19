package object

import (
	"encoding"
	"encoding/binary"
	"reflect"
)

type keySerializer[K any] interface {
	Marshal(K) []byte
	Unmarshal([]byte) K
}

func newKeySerializer[T any]() keySerializer[T] {
	var t T
	switch any(t).(type) {
	case int:
		return intKeySerializer[T]{}
	case int8:
		return int8KeySerializer[T]{}
	case int16:
		return int16KeySerializer[T]{}
	case int32:
		return int32KeySerializer[T]{}
	case int64:
		return int64KeySerializer[T]{}
	case uint:
		return uintKeySerializer[T]{}
	case uint8:
		return uint8KeySerializer[T]{}
	case uint16:
		return uint16KeySerializer[T]{}
	case uint32:
		return uint32KeySerializer[T]{}
	case uint64:
		return uint64KeySerializer[T]{}
	case string:
		return stringKeySerializer[T]{}
	case []byte:
		return bytesKeySerializer[T]{}
	case binarySerializable:
		return binaryKeySerializer[T]{}
	default:
		panic("unsupported type")
	}
}

type intKeySerializer[T any] struct{}

func (i intKeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(any(v).(int)))
}

func (i intKeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetInt(int64(binary.LittleEndian.Uint64(bz)))
	return t
}

type int8KeySerializer[T any] struct{}

func (i int8KeySerializer[T]) Marshal(v T) []byte {
	return []byte{byte(any(v).(int8))}
}

func (i int8KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetInt(int64(int8(bz[0])))
	return t
}

type int16KeySerializer[T any] struct{}

func (i int16KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint16(nil, uint16(any(v).(int16)))
}

func (i int16KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetInt(int64(binary.LittleEndian.Uint16(bz)))
	return t
}

type int32KeySerializer[T any] struct{}

func (i int32KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint32(nil, uint32(any(v).(int32)))
}

func (i int32KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetInt(int64(binary.LittleEndian.Uint32(bz)))
	return t
}

type int64KeySerializer[T any] struct{}

func (i int64KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(any(v).(int64)))
}

func (i int64KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetInt(int64(binary.LittleEndian.Uint64(bz)))
	return t
}

type uintKeySerializer[T any] struct{}

func (i uintKeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(any(v).(uint)))
}

func (i uintKeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetUint(uint64(binary.LittleEndian.Uint64(bz)))
	return t
}

type uint8KeySerializer[T any] struct{}

func (i uint8KeySerializer[T]) Marshal(v T) []byte {
	return []byte{byte(any(v).(uint8))}
}

func (i uint8KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetUint(uint64(bz[0]))
	return t
}

type uint16KeySerializer[T any] struct{}

func (i uint16KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint16(nil, uint16(any(v).(uint16)))
}

func (i uint16KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetUint(uint64(binary.LittleEndian.Uint16(bz)))
	return t
}

type uint32KeySerializer[T any] struct{}

func (i uint32KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint32(nil, uint32(any(v).(uint32)))
}

func (i uint32KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetUint(uint64(binary.LittleEndian.Uint32(bz)))
	return t
}

type uint64KeySerializer[T any] struct{}

func (i uint64KeySerializer[T]) Marshal(v T) []byte {
	return binary.LittleEndian.AppendUint64(nil, any(v).(uint64))
}

func (i uint64KeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetUint(binary.LittleEndian.Uint64(bz))
	return t
}

type stringKeySerializer[T any] struct{}

func (i stringKeySerializer[T]) Marshal(v T) []byte {
	return []byte(any(v).(string))
}

func (i stringKeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetString(string(bz))
	return t
}

type bytesKeySerializer[T any] struct{}

func (i bytesKeySerializer[T]) Marshal(v T) []byte {
	return any(v).([]byte)
}

func (i bytesKeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	reflect.ValueOf(&t).Elem().SetBytes(bz)
	return t
}

type binarySerializable interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type binaryKeySerializer[T any] struct{}

func (i binaryKeySerializer[T]) Marshal(v T) []byte {
	bz, err := any(&v).(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}
	return bz
}

func (i binaryKeySerializer[T]) Unmarshal(bz []byte) T {
	var t T
	if err := any(&t).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz); err != nil {
		panic(err)
	}
	return t
}
