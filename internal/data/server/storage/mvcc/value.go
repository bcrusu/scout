package mvcc

const (
	FlagEmpty     Flags = 0
	FlagTombstone Flags = 1
)

type Flags byte

func (f Flags) Tombstone() bool {
	return (f & FlagTombstone) != 0
}

func EncodeData(value []byte, flags Flags) []byte {
	return append(value, byte(flags))
}

func DecodeData(data []byte) ([]byte, Flags) {
	l := len(data)
	value := data[:l-1]
	flags := Flags(data[l-1])
	return value, flags
}
