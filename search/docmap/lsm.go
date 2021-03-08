package docmap

import (
	"fmt"
	"unsafe"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/lsm"
)

type lsmDocMap struct {
	lsm *lsm.Lsm
}

type lsmDocMapKey uint64
type lsmDocMapEntry struct {
	lsm.BaseEntry
	docID, inodeID, pitID uint64
}

func newlsmDocMap(cfg Cfg) DocMap {
	return &lsmDocMap{lsm.New(lsm.Config{
		Name:              cfg.Name,
		ID:                'a'<<16 + 'm'<<8 + 'p',
		Io:                cfg.IO,
		Comparator:        lsmDocMapEntryCmp,
		ComparatorWithKey: lsmDocMapEntryKeyCmp,
		Comparator2Keys:   lsmDocMapKeysCmp,
		CreateEntry:       func() lsm.Entry { return &lsmDocMapEntry{} },
		CreateEntryKey:    func() lsm.EntryKey { return new(lsmDocMapKey) },
		Cache:             cache.New(2 * 1024 * 1024),
		Log:               cfg.Log,
	})}
}

func (ldm *lsmDocMap) Insert(docID, inodeID, pitID uint64) (err error) {
	return ldm.lsm.Insert(&lsmDocMapEntry{docID: docID, inodeID: inodeID, pitID: pitID})
}

func (ldm *lsmDocMap) LookupInodeVersion(docID uint64) (inodeID, pitID uint64, err error) {
	key := lsmDocMapKey(docID)
	ent, err := ldm.lsm.Search(&key)
	if err != nil {
		return
	}
	if ent == nil {
		return 0, 0, ErrDocNotFound
	}
	aEnt := ent.(*lsmDocMapEntry)
	inodeID, pitID = aEnt.inodeID, aEnt.pitID
	return
}

func (ldm *lsmDocMap) WaitAllBackgroundInsertionRoutines() {
	ldm.lsm.WaitMergeDone()
}

func (ldm *lsmDocMap) WriteDesc(buf *lsm.SerializeBuf) {
	ldm.lsm.WriteDesc(buf)
}

func (ldm *lsmDocMap) ReadDesc(buf *lsm.DeserializeBuf) (err error) {
	return ldm.lsm.ReadDesc(buf)
}

func (ldmKey *lsmDocMapKey) Encode(buf *lsm.SerializeBuf) {
	buf.EncodeUint64(uint64(*ldmKey))
}

func (ldmKey *lsmDocMapKey) Decode(buf *lsm.DeserializeBuf) error {
	*ldmKey = lsmDocMapKey(buf.DecodeUint64())
	return buf.Error()
}

func (ldmKey *lsmDocMapKey) String() string {
	return fmt.Sprintf("%d", uint64(*ldmKey))
}

func (ldmKey *lsmDocMapKey) Size() int {
	return int(unsafe.Sizeof(*ldmKey))
}

func (ldmEntry *lsmDocMapEntry) Size() int {
	return int(unsafe.Sizeof(*ldmEntry))
}

func (ldmEntry *lsmDocMapEntry) Encode(buf *lsm.SerializeBuf, prevEntry lsm.Entry) {
	buf.EncodeUint64(ldmEntry.docID)
	buf.EncodeUint64(ldmEntry.inodeID)
	buf.EncodeUint64(ldmEntry.pitID)
}

func (ldmEntry *lsmDocMapEntry) Decode(buf *lsm.DeserializeBuf, prevEntry lsm.Entry) error {
	ldmEntry.docID = buf.DecodeUint64()
	ldmEntry.inodeID = buf.DecodeUint64()
	ldmEntry.pitID = buf.DecodeUint64()
	return buf.Error()
}

func (ldmEntry *lsmDocMapEntry) String() string {
	return fmt.Sprintf("{DocID: %d, InodeID: %d, PitID: %d}", ldmEntry.docID, ldmEntry.inodeID, ldmEntry.pitID)
}

func (ldmEntry *lsmDocMapEntry) GetKey() lsm.EntryKey {
	key := lsmDocMapKey(ldmEntry.docID)
	return &key
}

func lsmDocMapEntryCmp(a, b interface{}) int {
	e1 := a.(*lsmDocMapEntry)
	e2 := b.(*lsmDocMapEntry)
	return lsmDocMapCmp(e1.docID, e2.docID)
}

func lsmDocMapEntryKeyCmp(a, b interface{}) int {
	e1 := a.(*lsmDocMapEntry)
	k2 := b.(*lsmDocMapKey)
	return lsmDocMapCmp(e1.docID, uint64(*k2))
}

func lsmDocMapKeysCmp(a, b interface{}) int {
	k1 := a.(*lsmDocMapKey)
	k2 := b.(*lsmDocMapKey)
	return lsmDocMapCmp(uint64(*k1), uint64(*k2))
}

func lsmDocMapCmp(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
