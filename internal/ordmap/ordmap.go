package ordmap

import (
	"errors"
	"iter"
)

var (
	// ErrKeyExists is returned when a key already exists in the map.
	ErrKeyExists = errors.New("key already exists")
)

// Map is a map that maintains the order of the keys.
type Map[K comparable, V any] struct {
	m     map[K]V
	order []pair[K, V]
}

// pair is a key-value pair.
type pair[K, V any] struct {
	key   K
	value V
}

// New creates a new Map.
func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		m:     make(map[K]V),
		order: []pair[K, V]{},
	}
}

// Add adds a key-value pair to the map. it returns error if the key already exists.
func (m *Map[K, V]) Add(key K, value V) error {
	if _, ok := m.m[key]; ok {
		return ErrKeyExists
	}

	m.m[key] = value
	m.order = append(m.order, pair[K, V]{key: key, value: value})
	return nil
}

// Get returns the value of a key.
func (m *Map[K, V]) Get(key K) (V, bool) {
	value, ok := m.m[key]
	return value, ok
}

// Iter returns an iterator that iterates over all key-value pairs.
func (m *Map[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, p := range m.order {
			if !yield(p.key, p.value) {
				return
			}
		}
	}
}

// Len returns the number of key-value pairs in the map.
func (m *Map[K, V]) Len() int {
	return len(m.order)
}

// Delete deletes a key from the map.
func (m *Map[K, V]) Delete(key K) {
	delete(m.m, key)
	var newOrder []pair[K, V]
	for _, p := range m.order {
		if p.key != key {
			newOrder = append(newOrder, p)
		}
	}

	m.order = newOrder
}
