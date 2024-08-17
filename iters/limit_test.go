package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

func TestLimit(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := badgerutils.NewSerializedStore[StructA](txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Limit(store.NewIterator(badger.DefaultIteratorOptions), 1)
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	require.Equal(t, keys[0], iter.Item().Key())
	value, err := iter.Value()
	require.NoError(t, err)
	require.Equal(t, values[0], value)

	iter.Next()
	require.False(t, iter.Valid())
}
