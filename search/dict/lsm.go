package dict

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/neganovalexey/search/codeerrors"
	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/lsm"
)

var (
	// errTermNotFound returned on lookup if no term found
	errTermNotFound = codeerrors.ErrNotFound.WithMessage("term not found in Dictionary")
)

type lsmDict struct {
	lsm        *lsm.Lsm
	lastTermID FeatureID
	sync.RWMutex
}

type lsmDictEntryKey struct {
	field FieldID
	term  Term
}

type lsmDictEntry struct {
	term   Term
	termID FeatureID
	field  FieldID
	lsm.BaseEntry
}

const (
	lsmTermDictCacheSize = 15 * 1024 * 1024
)

func newLsmDict(cfg Cfg) *lsmDict {
	lsm := lsm.New(lsm.Config{
		Name:              cfg.Name,
		ID:                'd'<<16 + 'i'<<8 + 'c',
		Io:                cfg.IO,
		Comparator:        lsmDictEntryCmp,
		ComparatorWithKey: lsmDictEntryKeyCmp,
		Comparator2Keys:   lsmDictKeysCmp,
		CreateEntry:       func() lsm.Entry { return &lsmDictEntry{} },
		CreateEntryKey: func() lsm.EntryKey {
			var key lsmDictEntryKey
			return &key
		},
		Cache: cache.New(lsmTermDictCacheSize),
		Log:   cfg.Log,
	})
	return &lsmDict{lsm: lsm}
}

func (ldict *lsmDict) Get(field FieldID, term Term) (termID FeatureID, err error) {
	ldict.RLock()
	defer ldict.RUnlock()
	return ldict.lookupLocked(field, term)
}

func (ldict *lsmDict) lookupLocked(field FieldID, term Term) (termID FeatureID, err error) {
	key := lsmDictEntryKey{field, term}
	entry, err := ldict.lsm.Search(&key)
	if entry == nil {
		err = errTermNotFound
	}
	if err != nil {
		return
	}
	return entry.(*lsmDictEntry).termID, nil
}

func (ldict *lsmDict) WriteDesc(buf *lsm.SerializeBuf) {
	buf.EncodeFixedUint64(ldict.lastTermID)
	ldict.lsm.WriteDesc(buf)
}

func (ldict *lsmDict) ReadDesc(buf *lsm.DeserializeBuf) (err error) {
	ldict.lastTermID = buf.DecodeFixedUint64()
	err = ldict.lsm.ReadDesc(buf)
	return lsm.FirstErr(err, buf.Error())
}

func (ldict *lsmDict) GetOrInsert(field FieldID, term Term) (termID FeatureID, err error) {
	ldict.Lock()
	defer ldict.Unlock()
	termID, err = ldict.lookupLocked(field, term)
	if err == nil || !errors.Is(err, errTermNotFound) {
		return
	}
	ldict.lastTermID++
	termID = ldict.lastTermID
	err = ldict.lsm.Insert(&lsmDictEntry{term: term, field: field, termID: termID})
	return
}

func (ldict *lsmDict) WaitAllBackgroundInsertionRoutines() {
	ldict.lsm.WaitMergeDone()
}

func (l *lsmDictEntryKey) Encode(buf *lsm.SerializeBuf) {
	buf.EncodeUint32(uint32(l.field))
	buf.EncodeStr(l.term)
}

func (l *lsmDictEntryKey) Decode(buf *lsm.DeserializeBuf) error {
	f := buf.DecodeUint32()
	s := buf.DecodeStr()
	*l = lsmDictEntryKey{
		field: FieldID(f),
		term:  s,
	}
	return buf.Error()
}

func (l *lsmDictEntryKey) String() string {
	return fmt.Sprintf("{field: %d; term: %s}", l.field, l.term)
}

func (l *lsmDictEntryKey) Size() int {
	return int(unsafe.Sizeof(*l)) + len(l.term)
}

func (l *lsmDictEntry) Size() int {
	return int(unsafe.Sizeof(*l)) + len(l.term)
}

func (l *lsmDictEntry) Encode(buf *lsm.SerializeBuf, prevEntry lsm.Entry) {
	flags := uint8(0)
	encodeFrom := 0
	if prevEntry != nil {
		prev := prevEntry.(*lsmDictEntry)
		if strings.HasPrefix(l.term, prev.term) {
			flags |= 1
			encodeFrom = len(prev.term)
		}
	}
	// TODO: would be nice to encode w/o additional byte (i.e. 4bit length, 4bit flags)
	buf.EncodeFixedUint8(flags)
	buf.EncodeStr(l.term[encodeFrom:])
	buf.EncodeUint32(uint32(l.field))
	buf.EncodeUint64(l.termID)
}

func (l *lsmDictEntry) Decode(buf *lsm.DeserializeBuf, prevEntry lsm.Entry) error {
	flags := buf.DecodeFixedUint8()
	prefix := ""
	if flags&1 != 0 {
		prefix = prevEntry.(*lsmDictEntry).term
	}
	l.term = prefix + buf.DecodeStr()
	l.field = FieldID(buf.DecodeUint32())
	l.termID = buf.DecodeUint64()
	return buf.Error()
}

func (l *lsmDictEntry) String() string {
	return fmt.Sprintf("{Term: %v, FeatureID: %v}", l.term, l.termID)
}

func (l *lsmDictEntry) GetKey() lsm.EntryKey {
	return l.getKey()
}

func (l *lsmDictEntry) getKey() *lsmDictEntryKey {
	return &lsmDictEntryKey{field: l.field, term: l.term}
}

func lsmDictEntryCmp(a, b interface{}) int {
	e1 := a.(*lsmDictEntry)
	e2 := b.(*lsmDictEntry)
	return lsmDictKeysCmpTyped(e1.getKey(), e2.getKey())
}

func lsmDictEntryKeyCmp(a, b interface{}) int {
	e1 := a.(*lsmDictEntry)
	k2 := b.(*lsmDictEntryKey)
	return lsmDictKeysCmpTyped(e1.getKey(), k2)
}

func lsmDictKeysCmp(a, b interface{}) int {
	k1 := a.(*lsmDictEntryKey)
	k2 := b.(*lsmDictEntryKey)
	return lsmDictKeysCmpTyped(k1, k2)
}

func lsmDictKeysCmpTyped(k1, k2 *lsmDictEntryKey) int {
	switch {
	case k1.field < k2.field:
		return -1
	case k1.field > k2.field:
		return 1
	default:
		return strings.Compare(k1.term, k2.term)
	}
}
