package iters_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/iters"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/stretchr/testify/require"
)

func TestFirst(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := sstore.New[StructA](pstore.New(txn, []byte("v")))

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := store.NewIterator(badger.IteratorOptions{
		Prefix: []byte("foo"),
	})
	defer iter.Close()

	actual, err := iters.First(iter)
	require.NoError(t, err)
	require.Equal(t, values[0], actual)
}

func TestFirstItem(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = [][]byte{[]byte("bar1"), []byte("bar2")}
	)

	for i, key := range keys {
		err := txn.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := txn.NewIterator(badger.IteratorOptions{
		Prefix: []byte("foo"),
	})
	defer iter.Close()

	actual, err := iters.FirstItem(iter)
	require.NoError(t, err)
	require.Equal(t, keys[0], actual.Key())
	actualValue, err := actual.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, values[0], actualValue)
}
