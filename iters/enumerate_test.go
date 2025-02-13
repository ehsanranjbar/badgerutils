package iters_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

func TestEnumerate(t *testing.T) {
	it := iters.Enumerate[int8](iters.Slice([]int{1, 2, 3}))
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		v, _ := it.Value()
		require.Equal(t, int8(v)-1, it.Key())
	}
}
