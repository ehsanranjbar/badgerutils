package lex

import (
	"encoding/binary"
	"math"
)

// EncodeInt32 returns the byte slice representation of the given int32 which is encoded in lexicographical order.
func EncodeInt32(v int32) []byte {
	return EncodeUint32(uint32(v) ^ (1 << 31))
}

// DecodeInt32 returns the int32 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt32(b []byte) int32 {
	u := DecodeUint32(b)
	return int32(u ^ (1 << 31))
}

// EncodeUint32 returns the byte slice representation of the given uint32.
func EncodeUint32(v uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, v)
}

// DecodeUint32 returns the uint32 representation of the given byte slice.
func DecodeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// EncodeInt64 returns the byte slice representation of the given int64 which is encoded in lexicographical order.
func EncodeInt64(v int64) []byte {
	return EncodeUint64(uint64(v) ^ (1 << 63))
}

// DecodeInt64 returns the int64 representation of the given byte slice which is decoded from lexicographical order.
func DecodeInt64(b []byte) int64 {
	u := DecodeUint64(b)
	return int64(u ^ (1 << 63))
}

// EncodeUint64 returns the byte slice representation of the given uint64.
func EncodeUint64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

// DecodeUint64 returns the uint64 representation of the given byte slice.
func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// EncodeFloat32 returns the byte slice representation of the given float32 which is encoded in lexicographical order.
func EncodeFloat32(f float32) []byte {
	if math.IsNaN(float64(f)) {
		return EncodeUint32(0)
	}

	u := math.Float32bits(f)
	if f < 0 {
		u = ^u
	} else {
		u ^= (1 << 31)
	}

	return EncodeUint32(u)
}

// DecodeFloat32 returns the float32 representation of the given byte slice which is decoded from lexicographical order.
func DecodeFloat32(b []byte) float32 {
	u := DecodeUint32(b)
	if u == 0 {
		return float32(math.NaN())
	}

	if u < (1 << 31) {
		u = ^u
	} else {
		u ^= (1 << 31)
	}

	return math.Float32frombits(u)
}

// EncodeFloat64 returns the byte slice representation of the given float64 which is encoded in lexicographical order.
func EncodeFloat64(f float64) []byte {
	if math.IsNaN(f) {
		return EncodeUint64(0)
	}

	u := math.Float64bits(f)
	if f < 0 {
		u = ^u
	} else {
		u ^= (1 << 63)
	}

	return EncodeUint64(u)
}

// DecodeFloat64 returns the float64 representation of the given byte slice which is decoded from lexicographical order.
func DecodeFloat64(b []byte) float64 {
	u := DecodeUint64(b)
	if u == 0 {
		return math.NaN()
	}

	if u < (1 << 63) {
		u = ^u
	} else {
		u ^= (1 << 63)
	}

	return math.Float64frombits(u)
}
