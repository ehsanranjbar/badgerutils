package extensions_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils/extensions"
	"github.com/ehsanranjbar/badgerutils/iters"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestAssociateStore(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	store := pstore.New(txn, []byte("test"))
	as := extensions.NewAssociateStore(
		extensions.WithSynthFunc(
			extensions.MetadataSynthFunc[struct{}](true),
		),
	)

	t.Run("Init", func(t *testing.T) {
		err := as.Init(store, iters.Slice([]*struct{}{}))
		require.NoError(t, err)
	})

	t.Run("OnSet", func(t *testing.T) {
		err := as.OnSet([]byte("key"), nil, &struct{}{})
		require.NoError(t, err)
		metadata, err := as.Get([]byte("key"))
		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.Contains(t, *metadata, "created_at")
		require.Contains(t, *metadata, "updated_at")

		err = as.OnSet([]byte("key"), &struct{}{}, &struct{}{}, extensions.WithAssociateData(extensions.Metadata{"key": "value"}))
		require.NoError(t, err)
		metadata, err = as.Get([]byte("key"))
		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.Contains(t, *metadata, "key")
		require.Contains(t, *metadata, "created_at")
		require.Contains(t, *metadata, "updated_at")
	})

	t.Run("OnDelete", func(t *testing.T) {
		err := as.OnDelete([]byte("key"), &struct{}{})
		require.NoError(t, err)
		metadata, err := as.Get([]byte("key"))
		require.NoError(t, err)
		require.Nil(t, metadata)
	})

	t.Run("Drop", func(t *testing.T) {
		err := as.Drop()
		require.NoError(t, err)
	})
}
