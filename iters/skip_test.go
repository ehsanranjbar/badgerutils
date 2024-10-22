package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/iters"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestSkip(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	store := sstore.New[StructA](txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.SkipN(store.NewIterator(badger.DefaultIteratorOptions), 1)
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	require.Equal(t, keys[1], iter.Item().Key())
	value, err := iter.Value()
	require.NoError(t, err)
	require.Equal(t, values[1], value)
}
