package be

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeInt64Lex(t *testing.T) {
	tests := []struct {
		input    int64
		expected []byte
	}{
		{0, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{1, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{-1, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MaxInt64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{math.MinInt64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		result := EncodeInt64Lex(test.input)
		require.Equal(t, test.expected, result, "EncodeInt64Lex(%d)", test.input)
	}
}
func TestEncodeUint64Lex(t *testing.T) {
	tests := []struct {
		input    uint64
		expected []byte
	}{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, test := range tests {
		result := EncodeUint64Lex(test.input)
		require.Equal(t, test.expected, result, "EncodeUint64Lex(%d)", test.input)
	}
}

func TestInverseLex(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00}, []byte{0xff}},
		{[]byte{0xff}, []byte{0x00}},
		{[]byte{0x55, 0xaa}, []byte{0xaa, 0x55}},
		{[]byte{0x00, 0xff, 0x7f, 0x80}, []byte{0xff, 0x00, 0x80, 0x7f}},
	}

	for _, test := range tests {
		result := InverseLex(test.input)
		require.Equal(t, test.expected, result, "InverseLex(%v)", test.input)
	}
}

func TestIncrementLex(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0x01}, []byte{0x02}},
		{[]byte{0xff}, []byte{0xff, 0x01}},
		{[]byte{0x00, 0xff}, []byte{0x01, 0x00}},
		{[]byte{0xff, 0xff}, []byte{0xff, 0xff, 0x01}},
	}

	for _, test := range tests {
		result := IncrementLex(test.input)
		require.Equal(t, test.expected, result, "IncrementLex(%v)", test.input)
	}
}

func TestPadOrTruncLeft(t *testing.T) {
	tests := []struct {
		input    []byte
		length   int
		expected []byte
	}{
		{[]byte{0x01, 0x02, 0x03}, 5, []byte{0x00, 0x00, 0x01, 0x02, 0x03}},
		{[]byte{0x01, 0x02, 0x03}, 3, []byte{0x01, 0x02, 0x03}},
		{[]byte{0x01, 0x02, 0x03}, 2, []byte{0x02, 0x03}},
		{[]byte{0x01, 0x02, 0x03}, 0, []byte{}},
		{[]byte{}, 3, []byte{0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		result := PadOrTruncLeft(test.input, test.length)
		require.Equal(t, test.expected, result, "PadOrTruncLeft(%v, %d)", test.input, test.length)
	}
}

func TestPadOrTruncRight(t *testing.T) {
	tests := []struct {
		input    []byte
		length   int
		expected []byte
	}{
		{[]byte{0x01, 0x02, 0x03}, 5, []byte{0x01, 0x02, 0x03, 0x00, 0x00}},
		{[]byte{0x01, 0x02, 0x03}, 3, []byte{0x01, 0x02, 0x03}},
		{[]byte{0x01, 0x02, 0x03}, 2, []byte{0x01, 0x02}},
		{[]byte{0x01, 0x02, 0x03}, 0, []byte{}},
		{[]byte{}, 3, []byte{0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		result := PadOrTruncRight(test.input, test.length)
		require.Equal(t, test.expected, result, "PadOrTruncRight(%v, %d)", test.input, test.length)
	}
}
