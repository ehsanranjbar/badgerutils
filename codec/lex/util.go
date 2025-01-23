package lex

import "bytes"

// Invert returns the lexicographical inverse of the given byte slice in place.
func Invert(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		b[i] = ^b[i]
	}
	return b
}

// Increment increments the given byte slice so that it would be the next in lexicographical order.
func Increment(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] > 0x00 {
			return b
		}
	}
	return append(bytes.Repeat([]byte{0xff}, len(b)), 0x01)
}

// Decrement decrements the given byte slice so that it would be the previous in lexicographical order.
func Decrement(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]--
		if b[i] < 0xff {
			return b
		}
	}
	return bytes.Repeat([]byte{0x00}, len(b))
}
