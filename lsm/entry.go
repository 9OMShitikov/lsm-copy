package lsm

// EntryKey is lsm key
type EntryKey interface {
	Encode(buf *SerializeBuf)
	Decode(buf *DeserializeBuf) error
	String() string
	Size() int
}

// Entry is lsm [key+value]
type Entry interface {
	LsmDeleted() bool
	SetLsmDeleted(del bool)

	Size() int
	Encode(buf *SerializeBuf, prevEntry Entry)
	Decode(buf *DeserializeBuf, prevEntry Entry) error
	String() string

	GetKey() EntryKey
}

// EntryCreateFunc is entry maker function
type EntryCreateFunc func() Entry

// EntryKeyCreateFunc is entry key maker function
type EntryKeyCreateFunc func() EntryKey

// BaseEntry is base structure for embedding to real entries, implements LsmDeleted() and SetLsmDeleted()
type BaseEntry struct {
	lsmDelFlag bool // entry is marked as deleted in LSM tree
}

// LsmDeleted checks deleted flag
func (e *BaseEntry) LsmDeleted() bool {
	return e.lsmDelFlag
}

// SetLsmDeleted sets deleted flag
func (e *BaseEntry) SetLsmDeleted(d bool) {
	e.lsmDelFlag = d
}
