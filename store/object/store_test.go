package object_test

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/extutil"
	"github.com/ehsanranjbar/badgerutils/iters"
	objstore "github.com/ehsanranjbar/badgerutils/store/object"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	store, err := objstore.New[int64, testutil.TestStruct](txn)
	require.NoError(t, err)

	t.Run("SetWithNilId", func(t *testing.T) {
		err := store.Set(testutil.TestStruct{})
		require.Error(t, err)
	})

	txn.Discard()
	txn = testutil.PrepareTxn(t, true)

	var i int64 = 0
	store, err = objstore.New(
		txn,
		objstore.WithIDFunc(func(d *testutil.TestStruct) (int64, error) {
			i++
			return i, nil
		}),
		objstore.WithMetadataFunc[int64](extutil.MetadataSynthFunc[testutil.TestStruct](true)),
	)
	require.NoError(t, err)

	var (
		key1   int64 = 1
		key2   int64 = 3
		value1       = testutil.TestStruct{
			A: 1,
			B: "bar",
			F: &testutil.TestStruct{A: 10},
		}
		value2 = testutil.TestStruct{
			A: 2,
			B: "baz",
			D: []int{4, 5, 6},
			F: &testutil.TestStruct{A: 20},
		}
	)

	t.Run("NotFound", func(t *testing.T) {
		v, err := store.Get(key1)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		err := store.Set(value1)
		require.NoError(t, err)

		err = store.Set(value2, objstore.WithID[int64, testutil.TestStruct](key2), objstore.WithMetadata[int64, testutil.TestStruct](extutil.Metadata{"key": "value"}))
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		v, err := store.Get(key1)
		require.NoError(t, err)
		require.Equal(t, value1, *v)

		v, err = store.Get(key2)
		require.NoError(t, err)
		require.Equal(t, value2, *v)
	})

	t.Run("GetObject", func(t *testing.T) {
		obj, err := store.GetObject(key1)
		require.NoError(t, err)
		require.Equal(t, key1, *obj.ID)
		require.Equal(t, value1, obj.Data)

		obj, err = store.GetObject(key2)
		require.NoError(t, err)
		require.Equal(t, key2, *obj.ID)
		require.Equal(t, value2, obj.Data)
		require.Contains(t, obj.Metadata, "key")
	})

	t.Run("NewIterator", func(t *testing.T) {
		iter := store.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(2), c)
	})

	t.Run("Query", func(t *testing.T) {
		iter, err := store.Query("B like \"ba*\" and F.A > 10 and 4 in D")
		require.NoError(t, err)

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(1), c)
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(key1)
		require.NoError(t, err)

		_, err = store.Get(key1)
		require.Error(t, err)
	})
}
