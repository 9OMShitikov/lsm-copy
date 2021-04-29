package search

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
	aio "github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/lsm"
)

// BitLsm in LSM implementation for mapping abstract feature to bitmap. Since
// bitmap can be huge, it split into consecutive blocks, each block is described by
// first bit index and bitmap (i.e block covers bit range [first_bit_index...first_bit_index + bitmap.Length())
type BitLsm struct {
	lsm *lsm.Lsm
	BitLsmCfg
}

var _ Searcher = (*BitLsm)(nil)

// BitLsmCfg is config for bitLsm
// nolint: maligned
type BitLsmCfg struct {
	Name         string
	ID           uint32 // unique ID of LSM tree to verify that pages from correct tree are read
	Io           aio.LsmIo
	BitsPerBlock uint32           // each block for single feature fixed range of bits [offs...offs+BitsPerBlock)
	Cache        cache.BasicCache // node cache
	Log          *logrus.Logger
}

const (
	blsmMagic   = ('B' << 24) + ('L' << 16) + ('S' << 8) + 'M'
	blsmVersion = 1
)

// NewBitLsm create new bitLsm instance
func NewBitLsm(cfg BitLsmCfg) (blsm *BitLsm) {
	blsm = &BitLsm{BitLsmCfg: cfg}
	blsm.lsm = lsm.New(lsm.Config{
		Name:              cfg.Name,
		ID:                cfg.ID,
		Io:                cfg.Io,
		Comparator:        blsm.blsmEntryCmp,
		ComparatorWithKey: blsm.blsmEntryKeyCmp,
		Comparator2Keys:   blsm.blsmKeysCmp,
		MergeActionCb:     blsmEntryOR,
		CreateEntry:       blsm.newBlsmEntry,
		CreateEntryKey:    blsm.newBlsmEntryKey,
		Cache:             cfg.Cache,
		Log:               cfg.Log,
	})
	return
}

// WriteDesc serializes state of the bitlsm metadata
func (blsm *BitLsm) WriteDesc(buf *lsm.SerializeBuf) {
	buf.EncodeFixedUint32(blsmMagic)
	buf.EncodeFixedUint32(blsmVersion)
	buf.EncodeFixedUint32(blsm.BitsPerBlock)
	blsm.lsm.WriteDesc(buf)
}

// ReadDesc reads header needed for bitlsm state initialization (reads metadata)
func (blsm *BitLsm) ReadDesc(buf *lsm.DeserializeBuf) (err error) {
	magic := buf.DecodeFixedUint32()
	if magic != blsmMagic {
		return lsm.FirstErr(buf.Error(), fmt.Errorf("Invalid bitLsm Magic: %v", magic))
	}
	version := buf.DecodeFixedUint32()
	if version > blsmVersion {
		return lsm.FirstErr(buf.Error(), fmt.Errorf("Wrong blsm version: %v", version))
	}
	blsm.BitsPerBlock = buf.DecodeFixedUint32()
	if err = buf.Error(); err != nil {
		return err
	}
	return blsm.lsm.ReadDesc(buf)
}

// Insert new offs for given feature
func (blsm *BitLsm) Insert(feature FeatureID, field FieldID, bit uint64) (err error) {
	block, bitInBlock := BlockID(bit/uint64(blsm.BitsPerBlock)), bit%uint64(blsm.BitsPerBlock)
	bmap := roaring.BitmapOf(uint32(bitInBlock))
	bmap.SetCopyOnWrite(true)
	return blsm.lsm.Insert(&blsmEntry{
		feature: feature,
		field:   field,
		block:   block,
		bmap:    bmap,
	})
}

// GetBlockBitmap gets a bitmap for present feature/field blocks
// FIXME: store that bitmaps somehow
func (blsm *BitLsm) GetBlockBitmap(feature FeatureID, field FieldID) (*roaring.Bitmap, error) {
	result := roaring.New()
	it := blsm.Find(lsm.GE, feature, field, 0)
	for ; !it.Empty(); it.Next() {
		_, _, block, _ := it.Result()
		result.Add(block)
	}
	return result, nil
}

// WaitMergeDone waits until ongoing merge is complete
func (blsm *BitLsm) WaitMergeDone() {
	blsm.lsm.WaitMergeDone()
}

// MemUsage returns tree memory usage
func (blsm *BitLsm) MemUsage() int {
	return blsm.lsm.MemUsage()
}

// MergeEnable decrements merge disable level
func (blsm *BitLsm) MergeEnable() {
	blsm.lsm.MergeEnable()
}

// MergeDisable increments merge disable level
func (blsm *BitLsm) MergeDisable() {
	blsm.lsm.MergeDisable()
}

// Flush memory contents
func (blsm *BitLsm) Flush() error {
	return blsm.lsm.Flush()
}

// Merge starts merge from given ctree to next one
func (blsm *BitLsm) Merge(from, to int) error {
	return blsm.lsm.Merge(from, to)
}

// MergeAll starts sequential merge for all ctrees
func (blsm *BitLsm) MergeAll() error {
	return blsm.lsm.MergeAll()
}

// Close does blsm closing checks
func (blsm *BitLsm) Close() {
	blsm.lsm.Close()
}

// Error returns blsm error
func (blsm *BitLsm) Error() (err error) {
	return blsm.lsm.Error()
}

// Dump prints blsm entries in GE order
func (blsm *BitLsm) Dump() {
	it := BlsmIter{lsmIter: blsm.lsm.Find(lsm.GE, &blsmEntryKey{}), blsm: blsm}
	for ; !it.Empty(); it.Next() {
		fmt.Println(it.Result())
	}
}

// Name simply returns blsm name, with that it was initialized
func (blsm *BitLsm) Name() string {
	return blsm.lsm.Name()
}

// BitmapBlockSize tells how many bits are in the block
func (blsm *BitLsm) BitmapBlockSize() uint32 {
	return blsm.BitsPerBlock
}

// BlsmIter represents bitLsm iterator
type BlsmIter struct {
	lsmIter *lsm.Iter
	blsm    *BitLsm
}

// Empty checks iterator is empty
func (blsmIter *BlsmIter) Empty() bool {
	return blsmIter.lsmIter.Empty()
}

// Result at current position.
func (blsmIter *BlsmIter) Result() (FeatureID, FieldID, BlockID, *roaring.Bitmap) {
	entry := blsmIter.lsmIter.Result.(*blsmEntry)
	// TODO: can we remove Clone?
	return entry.feature, entry.field, entry.block, entry.bmap.Clone()
}

// Next moves iterator to next position
func (blsmIter *BlsmIter) Next() {
	blsmIter.lsmIter.Next()
}

// Error returns iterator error
func (blsmIter *BlsmIter) Error() error {
	return blsmIter.lsmIter.Error()
}

// Search acquires a particular bitmap block
func (blsm *BitLsm) Search(feature FeatureID, field FieldID, blockID BlockID) (*roaring.Bitmap, error) {
	iter := blsm.Find(lsm.EQ, feature, field, blockID)
	if err := iter.Error(); err != nil || iter.Empty() {
		return nil, err
	}
	_, _, _, result := iter.Result()
	return result, nil
}

// Find blocks in an LSM with given feature&field ID
func (blsm *BitLsm) Find(op int, feature FeatureID, field FieldID, from BlockID) BlockIterator {
	var lsmIter *lsm.Iter
	fromKey := &blsmEntryKey{feature: feature, field: field, block: from}
	switch op {
	case lsm.EQ:
		lsmIter = blsm.lsm.Find(lsm.EQ, fromKey)
	case lsm.GT, lsm.GE:
		toKey := &blsmEntryKey{feature: feature, field: field, block: math.MaxUint32}
		lsmIter = blsm.lsm.FindRange(op, fromKey, toKey)
	case lsm.LT, lsm.LE:
		toKey := &blsmEntryKey{feature: feature, field: field, block: 0}
		lsmIter = blsm.lsm.FindRange(op, fromKey, toKey)
	default:
		panic(fmt.Sprintf("Invalid Find operation %d", op))
	}
	return &BlsmIter{lsmIter: lsmIter, blsm: blsm}
}

// WaitAllBackgroundInsertionRoutines waits until all background insertion procedures not complete.
// Should be called before WriteDesc
func (blsm *BitLsm) WaitAllBackgroundInsertionRoutines() {
	blsm.WaitMergeDone()
}

type blsmEntry struct {
	feature FeatureID
	bmap    *roaring.Bitmap
	block   BlockID
	field   FieldID
	lsm.BaseEntry
}

type blsmEntryKey struct {
	feature FeatureID
	field   FieldID
	block   BlockID
}

func (blsm *BitLsm) newBlsmEntryKey() lsm.EntryKey {
	return &blsmEntryKey{}
}

func (blsm *BitLsm) newBlsmEntry() lsm.Entry {
	bmap := roaring.New()
	bmap.SetCopyOnWrite(true)
	return &blsmEntry{bmap: bmap}
}

func (blsm *BitLsm) blsmEntryCmp(a, b interface{}) int {
	e1 := a.(*blsmEntry)
	e2 := b.(*blsmEntry)
	return blsm.blsmCmp(e1.getKey(), e2.getKey())
}

func (blsm *BitLsm) blsmEntryKeyCmp(a, b interface{}) int {
	e1 := a.(*blsmEntry)
	k2 := b.(*blsmEntryKey)
	return blsm.blsmCmp(e1.getKey(), k2)
}

func (blsm *BitLsm) blsmKeysCmp(a, b interface{}) int {
	k1 := a.(*blsmEntryKey)
	k2 := b.(*blsmEntryKey)
	return blsm.blsmCmp(k1, k2)
}

func (blsm *BitLsm) blsmCmp(a, b *blsmEntryKey) int {
	switch {
	case a.feature < b.feature:
		return -1
	case a.feature > b.feature:
		return 1
	case a.field < b.field:
		return -1
	case a.field > b.field:
		return 1
	case a.block < b.block:
		return -1
	case a.block > b.block:
		return 1
	default:
		return 0
	}
}

func blsmEntryOR(new, existing interface{}) interface{} {
	e1 := new.(*blsmEntry)
	e2 := existing.(*blsmEntry)
	bmap := e1.bmap.Clone()
	bmap.Or(e2.bmap)
	bmap.SetCopyOnWrite(true)
	return &blsmEntry{feature: e1.feature, field: e1.field, block: e1.block, bmap: bmap}
}

func (blsmKey *blsmEntryKey) Encode(buf *lsm.SerializeBuf) {
	buf.EncodeUint64(blsmKey.feature)
	buf.EncodeFixedUint16(blsmKey.field)
	buf.EncodeUint32(blsmKey.block)
}

func (blsmKey *blsmEntryKey) Decode(buf *lsm.DeserializeBuf) (err error) {
	blsmKey.feature = buf.DecodeUint64()
	blsmKey.field = buf.DecodeFixedUint16()
	blsmKey.block = buf.DecodeUint32()
	return buf.Error()
}

func (blsmKey *blsmEntryKey) String() string {
	return fmt.Sprintf("{Feature: %d, field: %d, block: %v}", blsmKey.feature, blsmKey.field, blsmKey.block)
}

func (blsmKey *blsmEntryKey) Size() int {
	return int(unsafe.Sizeof(*blsmKey))
}

func (blsmEntry *blsmEntry) Size() int {
	return int(unsafe.Sizeof(*blsmEntry)) + int(blsmEntry.bmap.GetSizeInBytes())
}

func (blsmEntry *blsmEntry) Encode(buf *lsm.SerializeBuf, _ lsm.Entry) {
	buf.EncodeUint64(blsmEntry.feature)
	buf.EncodeFixedUint16(blsmEntry.field)
	buf.EncodeUint32(blsmEntry.block)
	bytes, err := blsmEntry.bmap.MarshalBinary()
	if err != nil {
		panic("Fatal error: " + err.Error())
	}
	buf.EncodeBuf(bytes)
}

func (blsmEntry *blsmEntry) Decode(buf *lsm.DeserializeBuf, _ lsm.Entry) (err error) {
	blsmEntry.feature = buf.DecodeUint64()
	blsmEntry.field = buf.DecodeFixedUint16()
	blsmEntry.block = buf.DecodeUint32()
	bytes := buf.DecodeBuf()
	err1 := blsmEntry.bmap.UnmarshalBinary(bytes)
	err2 := buf.Error()
	return lsm.FirstErr(err1, err2)
}

func (blsmEntry *blsmEntry) String() string {
	return fmt.Sprintf("{Feature: %d, field: %d, block: %v, Bitmap stats: %v}",
		blsmEntry.feature, blsmEntry.field, blsmEntry.block, blsmEntry.bmap.Stats())
}

func (blsmEntry *blsmEntry) getKey() *blsmEntryKey {
	return &blsmEntryKey{
		feature: blsmEntry.feature,
		field:   blsmEntry.field,
		block:   blsmEntry.block,
	}
}

func (blsmEntry *blsmEntry) GetKey() lsm.EntryKey {
	return blsmEntry.getKey()
}
