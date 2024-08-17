package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
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

	iter := iters.Aggregate(store.NewIterator(badger.DefaultIteratorOptions), func(state *int, value *StructA, item *badger.Item) *int {
		if state == nil {
			state = new(int)
		}

		*state += value.A
		return state
	})
	defer iter.Close()

	_, err = iters.Collect(iter)
	require.NoError(t, err)
	require.Equal(t, 3, *iter.Result())
}
