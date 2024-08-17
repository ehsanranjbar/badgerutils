package iters

import "github.com/ehsanranjbar/badgerutils"

// Collect collects all the items from the iterator and returns them as a slice.
func Collect[T any](it badgerutils.Iterator[T]) ([]T, error) {
	var items []T
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Value()
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, nil
}
