package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/iters"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := sstore.New[StructA](txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)
	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Filter(store.NewIterator(badger.DefaultIteratorOptions), func(value *StructA, item *badger.Item) bool {
		return value.A == 2
	})
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	require.Equal(t, keys[1], iter.Item().Key())
	value, err := iter.Value()
	require.NoError(t, err)
	require.Equal(t, values[1], value)

	iter.Next()
	require.False(t, iter.Valid())
}
