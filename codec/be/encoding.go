package be

import (
	"bytes"
	"encoding/binary"
	"math"
)

// EncodeInt64Lex returns the byte slice representation of the given int64 which is encoded in lexicographical order.
func EncodeInt64Lex(v int64) []byte {
	u := uint64(v) + math.MaxInt64
	if v < 0 {
		u = uint64(v + math.MaxInt64 + 1)
	}
	return EncodeUint64Lex(u)
}

// EncodeUint64Lex returns the byte slice representation of the given uint64.
func EncodeUint64Lex(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

// InverseLex returns the lexicographical inverse of the given byte slice in place.
func InverseLex(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		b[i] = ^b[i]
	}
	return b
}

// IncrementLex increments the given byte slice so that it would be the next in lexicographical order.
func IncrementLex(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] > 0x00 {
			return b
		}
	}
	return append(bytes.Repeat([]byte{0xff}, len(b)), 0x01)
}

// PadOrTruncLeft pads or trims the given byte slice to the given length from the left.
func PadOrTruncLeft(b []byte, n int) []byte {
	if len(b) == n {
		return b
	}
	if len(b) > n {
		return b[len(b)-n:]
	}
	return append(make([]byte, n-len(b)), b...)
}

// PadOrTruncRight pads or trims the given byte slice to the given length from the right.
func PadOrTruncRight(b []byte, n int) []byte {
	if len(b) == n {
		return b
	}
	if len(b) > n {
		return b[:n]
	}
	return append(b, make([]byte, n-len(b))...)
}
