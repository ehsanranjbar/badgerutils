package be

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
