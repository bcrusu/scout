package rocksdb

import (
	"bytes"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/linxGnu/grocksdb"
)

// Implements the same behavior as kv.Address.Compare:
// Keys are sorted ascending by keyspace and key and descending by timestamp.
// This results in a sort order with latest version at the start of each key range.
func newComparator() *grocksdb.Comparator {
	const tsSize = 8

	compareTs := func(a, b []byte) int {
		tsa := errors.Assert2(decodeUint64(a))
		tsb := errors.Assert2(decodeUint64(b))

		switch {
		case tsa < tsb:
			return -1
		case tsa > tsb:
			return 1
		default:
			return 0
		}
	}

	compareWithTs := func(a, b []byte) int {
		ax := a[:len(a)-tsSize]
		bx := b[:len(b)-tsSize]
		if x := bytes.Compare(ax, bx); x != 0 {
			return x
		}

		return -compareTs(a[len(a)-tsSize:], b[len(b)-tsSize:])
	}

	compareWithoutTs := func(a []byte, aHasTs bool, b []byte, bHasTs bool) int {
		ax := a
		if aHasTs {
			ax = a[:len(a)-tsSize]
		}

		bx := b
		if bHasTs {
			bx = b[:len(b)-tsSize]
		}

		return bytes.Compare(ax, bx)
	}

	return grocksdb.NewComparatorWithTimestamp(extensionName, tsSize, compareWithTs, compareTs, compareWithoutTs)
}
