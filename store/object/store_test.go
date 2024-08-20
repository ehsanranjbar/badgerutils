package object_test

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	objstore "github.com/ehsanranjbar/badgerutils/store/object"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	A int
	B string
}

func (t TestStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type TestIndexer struct{}

func (i TestIndexer) Index(v *TestStruct, update bool) map[string]badgerutils.RawKVPair {
	if v == nil {
		return nil
	}

	return map[string]badgerutils.RawKVPair{
		"A_idx": badgerutils.NewRawKVPair(binary.LittleEndian.AppendUint64(nil, uint64(-v.A)), nil),
		"B_idx": badgerutils.NewRawKVPair([]byte(v.B), nil),
	}
}

func TestObjectStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := objstore.New(txn, &TestIndexer{})

	var (
		keys    = [][]byte{{1}, {2}, {3}}
		objects = []*TestStruct{
			{A: 1, B: "foo"},
			{A: 2, B: "bar"},
			{A: 3, B: "baz"},
		}
	)

	t.Run("NotFound", func(t *testing.T) {
		for _, key := range keys {
			_, err := store.Get(key)
			require.Error(t, err)
		}
	})

	t.Run("Set", func(t *testing.T) {
		for i, key := range keys {
			err := store.Set(key, objects[i])
			require.NoError(t, err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		for i, key := range keys {
			v, err := store.Get(key)
			require.NoError(t, err)
			require.Equal(t, objects[i], v)
		}
	})

	t.Run("GetByRef", func(t *testing.T) {
		for _, obj := range objects {
			v, err := store.GetByRef("A_idx", binary.LittleEndian.AppendUint64(nil, uint64(-obj.A)))
			require.NoError(t, err)
			require.Equal(t, obj, v)
		}
	})

	t.Run("GetByRefNotFound", func(t *testing.T) {
		v, err := store.GetByRef("A_idx", binary.BigEndian.AppendUint64(nil, 0))
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Iterate", func(t *testing.T) {
		iter := store.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actual, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, objects, actual)
	})

	t.Run("IterateByRef", func(t *testing.T) {
		iter := store.NewRefIterator("A_idx", badger.DefaultIteratorOptions)
		defer iter.Close()

		actual, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{3}, {2}, {1}}, actual)
	})

	t.Run("Delete", func(t *testing.T) {
		for _, key := range keys {
			err := store.Delete(key)
			require.NoError(t, err)
		}
	})
}
