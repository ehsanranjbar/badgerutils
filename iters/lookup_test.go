package iters_test

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/iters"
	objstore "github.com/ehsanranjbar/badgerutils/store/object"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	"github.com/stretchr/testify/require"
)

type TestIndexer struct{}

func (i TestIndexer) Index(v *StructA, update bool) map[string]refstore.RefEntry {
	if v == nil {
		return nil
	}

	return map[string]refstore.RefEntry{
		"A_idx": refstore.NewRefEntry(binary.LittleEndian.AppendUint64(nil, uint64(v.A))),
	}
}

func TestLookupIterator(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := objstore.New[int](txn, &TestIndexer{})

	var (
		keys   = []int{1, 2}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Lookup(store, store.NewRefIterator("A_idx", badger.DefaultIteratorOptions))
	defer iter.Close()

	actual, err := iters.Collect(iter)
	require.NoError(t, err)
	require.Len(t, actual, 2)
	require.Equal(t, values, actual)
}
