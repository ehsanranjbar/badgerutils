package testutil

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// PrepareTxn creates an in-memory BadgerDB and a transaction for testing.
func PrepareTxn(t testing.TB, update bool) *badger.Txn {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	txn := db.NewTransaction(update)
	t.Cleanup(func() {
		txn.Discard()
	})
	return txn
}
