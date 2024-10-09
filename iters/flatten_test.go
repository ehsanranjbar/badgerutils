package iters_test

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	var its []badgerutils.Iterator[*StructA]
	for i := 0; i < 3; i++ {
		store := sstore.New[StructA](pstore.New(txn, []byte{byte(i)}))

		for j := 0; j < 3; j++ {
			err := store.Set([]byte{byte(j)}, &StructA{A: i*3 + j})
			require.NoError(t, err)

		}

		its = append(its, sstore.NewIterator[StructA](store.NewIterator(badger.DefaultIteratorOptions)))
	}

	flatten := iters.Flatten(iters.Slice(its))
	defer flatten.Close()
	var i, j int
	for flatten.Rewind(); flatten.Valid(); flatten.Next() {
		require.Equal(t, []byte{byte(i), byte(j)}, flatten.Item().Key())

		value, err := flatten.Value()
		require.NoError(t, err)

		assert.Equal(t, &StructA{A: i*3 + j}, value)

		j++
		if j == 3 {
			j = 0
			i++
		}
	}

	flatten.Seek(binary.LittleEndian.AppendUint64(nil, 1))
	require.Equal(t, []byte{1, 0}, flatten.Item().Key())
}
