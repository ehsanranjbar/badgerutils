package be

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

// EncodeFunc is a function that encodes a value to a byte slice.
type EncodeFunc func(v any) []byte

// GetEncodeFuncByType returns a function that encodes a value of the given type to a byte slice.
func GetEncodeFuncByType(t reflect.Type) EncodeFunc {
	switch t.Kind() {
	case reflect.String:
		return func(v any) []byte {
			return EncodeString(v.(string))
		}
	case reflect.Int:
		return func(v any) []byte {
			return EncodeInt64(int64(v.(int)))
		}
	case reflect.Uint:
		return func(v any) []byte {
			return binary.LittleEndian.AppendUint64(nil, uint64(v.(uint)))
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return func(v any) []byte {
				return v.([]byte)
			}
		}
	}
	panic(fmt.Sprintf("unsupported type %s", t.Kind()))
}

// EncodeString returns the byte slice representation of the given string.
func EncodeString(v string) []byte {
	return []byte(v)
}

// EncodeInt64 returns the byte slice representation of the given int64.
func EncodeInt64(v int64) []byte {
	u := uint64(v) + math.MaxInt64
	if v < 0 {
		u = uint64(v + math.MaxInt64)
	}
	return binary.LittleEndian.AppendUint64(nil, u)
}

// InverseBytes returns the boolean inverse of the given byte slice.
func InverseBytes(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		b[i] = ^b[i]
	}
	return b
}
