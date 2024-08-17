package iters_test

import (
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

type MockIterator[T any] struct {
	Items []T
	i     int
	err   error
}

func NewMockIterator[T any](items []T) *MockIterator[T] {
	return &MockIterator[T]{Items: items}
}

func (it *MockIterator[T]) Close() {}

func (it *MockIterator[T]) Item() *badger.Item {
	return nil
}

func (it *MockIterator[T]) Next() {
	it.i++
}

func (it *MockIterator[T]) Rewind() {
	it.i = 0
}

func (it *MockIterator[T]) Seek(key []byte) {}

func (it *MockIterator[T]) Valid() bool {
	return it.i < len(it.Items)
}

func (it *MockIterator[T]) Value() (value T, err error) {
	if it.i < len(it.Items) {
		return it.Items[it.i], it.err
	}
	return value, fmt.Errorf("out of bounds")
}

func TestCollect(t *testing.T) {
	var (
		items = []int{1, 2, 3}
		it    = NewMockIterator(items)
	)

	collected, err := iters.Collect(it)
	require.NoError(t, err)
	require.Equal(t, items, collected)
}
