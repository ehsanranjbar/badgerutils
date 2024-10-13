package exprs_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/stretchr/testify/require"
)

func TestRangeString(t *testing.T) {
	tests := []struct {
		partition exprs.Range[[]byte]
		expected  string
	}{
		{
			partition: exprs.NewRange(exprs.NewBound([]byte{0x01}, false), exprs.NewBound([]byte{0x02}, false)),
			expected:  "[0x01, 0x02]",
		},
		{
			partition: exprs.NewRange(exprs.NewBound[[]byte]([]byte{0x01}, true), exprs.NewBound[[]byte]([]byte{0x02}, true)),
			expected:  "(0x01, 0x02)",
		},
		{
			partition: exprs.NewRange[[]byte](nil, nil),
			expected:  "[0x00, ∞)",
		},
	}

	for _, test := range tests {
		result := test.partition.String()
		require.Equal(t, test.expected, result)
	}
}
