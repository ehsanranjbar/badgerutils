package serialized_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	"github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	store := serialized.New[TestStruct](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*TestStruct{{A: 1, B: "bar1"}, {A: 2, B: "bar2"}}
	)
	for i, key := range keys {
		err := ins.Set(key, values[i])
		require.NoError(t, err)
	}

	t.Run("Iterate", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		var (
			actualKeys   [][]byte
			actualValues []*TestStruct
		)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			require.NotNil(t, item)

			v, err := iter.Value()
			require.NoError(t, err)
			actualKeys = append(actualKeys, iter.Key())
			actualValues = append(actualValues, v)
		}
		require.Equal(t, 2, len(actualKeys))
		require.Equal(t, 2, len(actualValues))
		require.Equal(t, keys, actualKeys)
		require.Equal(t, values, actualValues)
	})

	t.Run("IteratePrefix", func(t *testing.T) {
		store := serialized.New[TestStruct](pstore.New(nil, []byte("foo")))

		ins := store.Instantiate(txn)
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		var (
			actualKeys   [][]byte
			actualValues []*TestStruct
		)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			require.NotNil(t, item)

			v, err := iter.Value()
			require.NoError(t, err)
			actualKeys = append(actualKeys, iter.Key())
			actualValues = append(actualValues, v)
		}

		require.Equal(t, 2, len(actualKeys))
		require.Equal(t, 2, len(actualValues))
		require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, actualKeys)
		require.Equal(t, values, actualValues)
	})
}
