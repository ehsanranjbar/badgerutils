package serialized_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/store/serialized"
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

func (t TestStruct) TTL() time.Duration {
	return time.Minute
}

func (t TestStruct) MetaByte() byte {
	return 0xff
}

type FailStruct struct{}

func (f FailStruct) MarshalBinary() ([]byte, error) {
	return nil, fmt.Errorf("failed to marshal")
}

func (f *FailStruct) UnmarshalBinary(data []byte) error {
	return fmt.Errorf("failed to unmarshal")
}

func TestSerializedStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := serialized.New[TestStruct](txn)
	failStore := serialized.New[FailStruct](txn)

	var (
		key   = []byte("foo")
		value = &TestStruct{A: 1, B: "bar"}
	)

	t.Run("NotFound", func(t *testing.T) {
		v, err := store.Get(key)
		require.Error(t, err)
		require.Nil(t, v)
	})

	t.Run("Set", func(t *testing.T) {
		err = store.Set(key, value)
		require.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		actual, err := store.Get(key)
		require.NoError(t, err)
		require.Equal(t, value, actual)
	})

	t.Run("GetWithItem", func(t *testing.T) {
		item, actual, err := store.GetWithItem(key)
		require.NoError(t, err)
		require.Equal(t, value, actual)
		require.NotZero(t, item.ExpiresAt())
		require.Equal(t, byte(0xff), item.UserMeta())
	})

	t.Run("MarshalFail", func(t *testing.T) {
		_, err := failStore.Get(key)
		require.Error(t, err)
	})

	t.Run("UnmarshalFail", func(t *testing.T) {
		err = failStore.Set(key, &FailStruct{})
		require.Error(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		err = store.Delete(key)
		require.NoError(t, err)
	})
}
