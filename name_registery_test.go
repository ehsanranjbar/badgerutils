package badgerutils_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	biters "github.com/ehsanranjbar/badgerutils/iters/bare"
	"github.com/stretchr/testify/require"
)

func TestNameRegistry(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	nreg, err := badgerutils.NewNameRegistry(db)
	t.Run("Init", func(t *testing.T) {
		require.NoError(t, err)

		prefix, err := nreg.Name("foo")
		require.NoError(t, err)
		require.Equal(t, []byte{0x01}, prefix)

		prefix = nreg.MustName("bar")
		require.Equal(t, []byte{0x02}, prefix)
	})

	t.Run("Reload", func(t *testing.T) {
		nreg2, err := badgerutils.NewNameRegistry(db)
		require.NoError(t, err)

		prefix, err := nreg2.Name("bar")
		require.NoError(t, err)
		require.Equal(t, []byte{0x02}, prefix)
	})

	t.Run("SubRegistry", func(t *testing.T) {
		nreg2, err := badgerutils.NewNameRegistry(db, badgerutils.WithRegistryPrefix(nreg.MustName("branch")))
		require.NoError(t, err)

		prefix, err := nreg2.Name("foo")
		require.NoError(t, err)
		require.Equal(t, []byte{0x01}, prefix)

		prefix = nreg2.MustName("bar")
		require.Equal(t, []byte{0x02}, prefix)
	})

	t.Run("Keys", func(t *testing.T) {
		var keys [][]byte
		err := db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			keys = biters.CollectKeys(it)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t,[][]byte{
			{0x00},
			{0x03},
		}, keys)
	})
}
