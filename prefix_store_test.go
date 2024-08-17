package badgerutils_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/badgerutils"
)

func TestPrefixStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := badgerutils.NewPrefixStore(txn, []byte("prefix"))

	var (
		key   = []byte("foo")
		value = []byte("bar")
	)

	t.Run("Set", func(t *testing.T) {
		err := store.Set(key, value)
		require.NoError(t, err)
	})

	t.Run("SetEntry", func(t *testing.T) {
		err := store.SetEntry(&badger.Entry{Key: key, Value: value})
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		item, err := store.Get(key)
		require.NoError(t, err)
		require.NotNil(t, item)
		item, err = txn.Get(append([]byte("prefix"), key...))
		require.NoError(t, err)
		require.NotNil(t, item)
	})

	t.Run("NewIterator", func(t *testing.T) {
		iter := store.NewIterator(badger.IteratorOptions{Prefix: []byte("foo")})
		defer iter.Close()
		require.NotNil(t, iter)

		for iter.Rewind(); iter.Valid(); iter.Next() {
			iter.Item().Value(func(val []byte) error {
				require.Equal(t, value, val)
				return nil
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(key)
		require.NoError(t, err)
	})

	t.Run("Get after Delete", func(t *testing.T) {
		item, err := store.Get(key)
		require.Error(t, err)
		require.Nil(t, item)
	})
}
