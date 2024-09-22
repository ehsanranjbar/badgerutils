package extensible_test

import (
	"encoding/json"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	extstore "github.com/ehsanranjbar/badgerutils/store/extensible"
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

func (i TestIndexer) Index(v *TestStruct, update bool) []badgerutils.RawKVPair {
	return []badgerutils.RawKVPair{}
}

func (i TestIndexer) Lookup(args ...any) (badgerutils.Iterator[indexing.Partition], error) {
	return nil, nil
}

func TestObjectStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := extstore.New[TestStruct](txn)
	idx := TestIndexer{}
	ext := indexing.NewExtension(idx)
	store.AddExtension("test", ext)

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

	t.Run("Iterate", func(t *testing.T) {
		iter := store.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		actual, err := iters.Collect(iter)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, objects, actual)
	})

	t.Run("Delete", func(t *testing.T) {
		for _, key := range keys {
			err := store.Delete(key)
			require.NoError(t, err)
		}
	})
}
