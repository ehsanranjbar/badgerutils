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
	store, err := objstore.New[int64, testutil.TestStruct](nil)
	require.NoError(t, err)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	t.Run("SetWithNilId", func(t *testing.T) {
		err := ins.Set(testutil.TestStruct{})
		require.Error(t, err)
	})

	var i int64 = 0
	store, err = objstore.New(
		nil,
		objstore.WithIdFunc(func(d *testutil.TestStruct) (int64, error) {
			i++
			return i, nil
		}),
		objstore.WithMetadataFunc[int64](extutil.MetadataSynthFunc[testutil.TestStruct](true)),
	)
	require.NoError(t, err)

	txn.Discard()
	txn = testutil.PrepareTxn(t, true)
	ins = store.Instantiate(txn)

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
		v, err := ins.Get(key1)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		err := ins.Set(value1)
		require.NoError(t, err)

		err = ins.Set(value2, objstore.WithId[int64, testutil.TestStruct](key2), objstore.WithMetadata[int64, testutil.TestStruct](extutil.Metadata{"key": "value"}))
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		v, err := ins.Get(key1)
		require.NoError(t, err)
		require.Equal(t, value1, *v)

		v, err = ins.Get(key2)
		require.NoError(t, err)
		require.Equal(t, value2, *v)
	})

	t.Run("GetObject", func(t *testing.T) {
		obj, err := ins.GetObject(key1)
		require.NoError(t, err)
		require.Equal(t, key1, *obj.Id)
		require.Equal(t, value1, obj.Data)

		obj, err = ins.GetObject(key2)
		require.NoError(t, err)
		require.Equal(t, key2, *obj.Id)
		require.Equal(t, value2, obj.Data)
		require.Contains(t, obj.Metadata, "key")
	})

	t.Run("NewIterator", func(t *testing.T) {
		iter := ins.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(2), c)
	})

	t.Run("Query", func(t *testing.T) {
		iter, err := ins.Query("B like \"ba*\" and F.A > 10 and 4 in D")
		require.NoError(t, err)

		c := iters.ConsumeAndCount(iter)
		require.Equal(t, uint(1), c)
	})

	t.Run("Delete", func(t *testing.T) {
		err := ins.Delete(key1)
		require.NoError(t, err)

		_, err = ins.Get(key1)
		require.Error(t, err)
	})
}
