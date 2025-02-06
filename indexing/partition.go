package indexing

import (
	"bytes"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec/lex"
	"github.com/ehsanranjbar/badgerutils/expr"
	"github.com/ehsanranjbar/badgerutils/iters"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
)

// Partition represents a range of keys from low to high with optional exclusivity on both ends.
type Partition = expr.Range[[]byte]

// NewPartition creates a new partition with the given low and high keys and exclusivity.
func NewPartition(low, high *expr.Bound[[]byte]) Partition {
	return expr.NewRange(low, high)
}

// LookupPartitions returns an iterator that iterates over the keys in the given partition iterator.
func LookupPartitions(
	store *refstore.Instance,
	parts badgerutils.Iterator[[]byte, Partition],
	opts badger.IteratorOptions,
) badgerutils.Iterator[[]byte, []byte] {
	return iters.Flatten(
		iters.Map(parts, func(p Partition, _ *badger.Item) (badgerutils.Iterator[[]byte, []byte], error) {
			// Omitting the prefix option.
			iter := store.NewIterator(badger.IteratorOptions{
				PrefetchSize:   opts.PrefetchSize,
				PrefetchValues: opts.PrefetchValues,
				Reverse:        opts.Reverse,
				AllVersions:    opts.AllVersions,
				InternalAccess: opts.InternalAccess,
				SinceTs:        opts.SinceTs,
			})

			if opts.Reverse {
				// Special case for reverse iteration with non-empty high bound that happens with reference stores
				// Because we want to seek and sever base on prefixes instead of the actual keys.
				var s []byte = nil
				if !p.High().IsEmpty() {
					s = p.High().Value()

					if !p.High().Exclusive() {
						s = lex.Increment(bytes.Clone(p.High().Value()))
					}
				}
				iter = iters.RewindSeek(iter, s)

				if !p.High().IsEmpty() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						if p.High().Exclusive() {
							return struct{}{}, bytes.Compare(key, p.High().Value()) >= 0
						} else {
							return struct{}{}, bytes.Compare(key, p.High().Value()) > 0
						}
					})
				}
			} else {
				var e []byte = nil
				if !p.Low().IsEmpty() {
					e = p.Low().Value()
				}
				iter = iters.RewindSeek(iter, e)

				if !p.Low().IsEmpty() && p.Low().Exclusive() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						return struct{}{}, bytes.Equal(key, p.Low().Value())
					})
				}
			}

			return iters.Sever(iter, func(key []byte, _ []byte, _ *badger.Item) bool {
				if opts.Reverse {
					if p.Low().IsEmpty() {
						return false
					}

					if p.Low().Exclusive() {
						return bytes.Compare(key, p.Low().Value()) <= 0
					} else {
						return bytes.Compare(key, p.Low().Value()) < 0
					}
				} else {
					if p.High().IsEmpty() {
						return false
					}

					if p.High().Exclusive() {
						return bytes.Compare(key, p.High().Value()) >= 0
					} else {
						return bytes.Compare(key, p.High().Value()) > 0
					}
				}
			}), nil
		}),
	)
}
