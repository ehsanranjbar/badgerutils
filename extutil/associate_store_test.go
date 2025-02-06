package extutil_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils/extutil"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestAssociateStore(t *testing.T) {
	as := extutil.NewAssociateStore(
		extutil.WithSynthFunc(
			extutil.MetadataSynthFunc[struct{}](true),
		),
	)
	store := pstore.New(nil, []byte("test"))
	as.Init(store)

	txn := testutil.PrepareTxn(t, true)
	ins := as.Instantiate(txn).(*extutil.AssociateStoreInstance[struct{}, extutil.Metadata, *extutil.Metadata])

	t.Run("OnSet", func(t *testing.T) {
		err := ins.OnSet([]byte("key"), nil, &struct{}{})
		require.NoError(t, err)
		metadata, err := ins.Get([]byte("key"))
		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.Contains(t, *metadata, "created_at")
		require.Contains(t, *metadata, "updated_at")

		err = ins.OnSet([]byte("key"), &struct{}{}, &struct{}{}, extutil.WithAssociateData(extutil.Metadata{"key": "value"}))
		require.NoError(t, err)
		metadata, err = ins.Get([]byte("key"))
		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.Contains(t, *metadata, "key")
		require.Contains(t, *metadata, "created_at")
		require.Contains(t, *metadata, "updated_at")
	})

	t.Run("OnDelete", func(t *testing.T) {
		err := ins.OnDelete([]byte("key"), &struct{}{})
		require.NoError(t, err)
		metadata, err := ins.Get([]byte("key"))
		require.NoError(t, err)
		require.Nil(t, metadata)
	})
}
