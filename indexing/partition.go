package indexing

import (
	"bytes"
	"encoding/hex"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	"github.com/ehsanranjbar/badgerutils/utils/be"
)

// Partition represents a range of keys from low to high with optional exclusivity on both ends.
type Partition struct {
	low, high Bound[[]byte]
}

// NewPartition creates a new partition with the given low and high keys and exclusivity.
func NewPartition(low, high Bound[[]byte]) Partition {
	return Partition{
		low:  low,
		high: high,
	}
}

// String returns the string representation of the partition.
func (p Partition) String() string {
	var sb strings.Builder
	if p.low.exclusive {
		sb.WriteString("(")
	} else {
		sb.WriteString("[")
	}
	if p.low.IsEmpty() {
		sb.WriteString("0x00")
	} else {
		sb.WriteString("0x")
		sb.WriteString(hex.EncodeToString(p.low.value))
	}
	sb.WriteString(", ")
	if p.high.IsEmpty() {
		sb.WriteString("∞")
	} else {
		sb.WriteString("0x")
		sb.WriteString(hex.EncodeToString(p.high.value))
	}
	if p.high.exclusive {
		sb.WriteString(")")
	} else {
		sb.WriteString("]")
	}
	return sb.String()
}

// LookupPartitions returns an iterator that iterates over the keys in the given partition iterator.
func LookupPartitions(
	store *refstore.Store,
	parts badgerutils.Iterator[Partition],
	opts badger.IteratorOptions,
) badgerutils.Iterator[[]byte] {
	return iters.Flatten(
		iters.Map(parts, func(p Partition, _ *badger.Item) (badgerutils.Iterator[[]byte], error) {
			iter := store.NewIterator(badger.IteratorOptions{
				PrefetchSize:   opts.PrefetchSize,
				PrefetchValues: opts.PrefetchValues,
				Reverse:        opts.Reverse,
				AllVersions:    opts.AllVersions,
				InternalAccess: opts.InternalAccess,
				SinceTs:        opts.SinceTs,
			})

			if opts.Reverse {
				s := p.high.value
				if !p.high.IsEmpty() && !p.high.Exclusive() {
					s = be.IncrementBytes(bytes.Clone(p.high.value))
				}

				iter = iters.RewindSeek(iter, s)
				if !p.high.IsEmpty() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						if p.high.Exclusive() {
							return struct{}{}, bytes.Compare(key, p.high.value) >= 0
						} else {
							return struct{}{}, bytes.Compare(key, p.high.value) > 0
						}
					})
				}
			} else {
				iter = iters.RewindSeek(iter, p.low.value)
				if p.low.Exclusive() {
					iter = iters.Skip(iter, func(_ struct{}, key []byte, _ []byte, _ *badger.Item) (struct{}, bool) {
						return struct{}{}, bytes.Equal(key, p.low.value)
					})
				}
			}

			return iters.Sever(iter, func(key []byte, _ []byte, _ *badger.Item) bool {
				if opts.Reverse {
					if p.low.IsEmpty() {
						return false
					}

					if p.low.Exclusive() {
						return bytes.Compare(key, p.low.value) <= 0
					} else {
						return bytes.Compare(key, p.low.value) < 0
					}
				} else {
					if p.high.IsEmpty() {
						return false
					}

					if p.high.Exclusive() {
						return bytes.Compare(key, p.high.value) >= 0
					} else {
						return bytes.Compare(key, p.high.value) > 0
					}
				}
			}), nil
		}),
	)
}
