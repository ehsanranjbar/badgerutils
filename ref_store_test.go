package badgerutils_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	badgerutils "github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

func TestRefStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := badgerutils.NewRefStore(txn)

	var (
		prefix = []byte("prefix")
		key    = []byte("foo")
		value  = []byte("bar")
	)

	t.Run("Set", func(t *testing.T) {
		err := store.Set(key, badgerutils.NewRefEntry(prefix).WithValue(value))
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		ref, err := store.Get(append(prefix, key...))
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, prefix, ref.Prefix)
		require.Equal(t, key, ref.Key)
	})

	t.Run("NotFound", func(t *testing.T) {
		v, err := store.Get(key)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Iterate", func(t *testing.T) {
		iter := store.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		refs, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, prefix, refs[0].Prefix)
		require.Equal(t, key, refs[0].Key)
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(append(prefix, key...))
		require.NoError(t, err)
	})
}
