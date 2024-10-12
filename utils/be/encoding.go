package be

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

// EncodeFunc is a function that encodes a value to a byte slice.
type EncodeFunc func(v any) ([]byte, error)

// GetEncodeFunc returns a function that encodes a value of the given type to a byte slice.
func GetEncodeFunc(t reflect.Type) EncodeFunc {
	switch t.Kind() {
	case reflect.String:
		return func(v any) ([]byte, error) {
			return EncodeString(v.(string)), nil
		}
	case reflect.Int:
		return func(v any) ([]byte, error) {
			return EncodeInt64(int64(v.(int))), nil
		}
	case reflect.Uint:
		return func(v any) ([]byte, error) {
			return EncodeUint64(uint64(v.(uint))), nil
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return func(v any) ([]byte, error) {
				return v.([]byte), nil
			}
		}
	case reflect.Pointer:
		return GetEncodeFunc(t.Elem())
	}
	panic(fmt.Sprintf("unsupported type %s", t))
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
	return EncodeUint64(u)
}

// EncodeUint64 returns the byte slice representation of the given uint64.
func EncodeUint64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

// Inverse returns the boolean inverse of the given byte slice.
func Inverse(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		b[i] = ^b[i]
	}
	return b
}

// Increment increments the given byte slice so that it would be the next in lexicographical order.
func Increment(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return b
		}
	}
	return append(b, 0x01)
}

// PadOrTrimLeft pads or trims the given byte slice to the given length from the left.
func PadOrTrimLeft(b []byte, l int) []byte {
	if len(b) == l {
		return b
	}
	if len(b) > l {
		return b[l-len(b):]
	}
	return append(make([]byte, l-len(b)), b...)
}

// PadOrTrimRight pads or trims the given byte slice to the given length from the right.
func PadOrTrimRight(b []byte, l int) []byte {
	if len(b) == l {
		return b
	}
	if len(b) > l {
		return b[:l]
	}
	return append(b, make([]byte, l-len(b))...)
}

// GetEncodeSize returns the size of the given type. 0 is returned if the size is unknown.
func GetEncodeSize(t reflect.Type) int {
	switch t.Kind() {
	case reflect.Int | reflect.Int64 | reflect.Uint | reflect.Uint64:
		return 8
	}
	return 0
}
