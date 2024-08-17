package iters_test

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

func TestLookupIterator(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	vstore := badgerutils.NewSerializedStore[StructA](badgerutils.NewPrefixStore(txn, []byte("v")))
	rstore := badgerutils.NewRefStore(badgerutils.NewPrefixStore(txn, []byte("r")))

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 2}, {A: 1}}
	)

	for i, key := range keys {
		err := vstore.Set(key, values[i])
		require.NoError(t, err)

		err = rstore.Set(key, badgerutils.NewRefEntry(binary.AppendUvarint(nil, uint64(values[i].A))))
		require.NoError(t, err)
	}

	iter := iters.Lookup(vstore, rstore.NewIterator(badger.DefaultIteratorOptions))
	defer iter.Close()

	actual, err := iters.Collect(iter)
	require.NoError(t, err)
	require.Len(t, actual, 2)
	require.Equal(t, values[1], actual[0])
	require.Equal(t, values[0], actual[1])
}
