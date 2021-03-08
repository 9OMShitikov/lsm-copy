package lsm

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
	aio "github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/rbtree"
)

const lsmMagic uint32 = 0x52de2340
const lsmVersion uint32 = 2
const maxLsmCtrees = 12
const memLsmCtrees = 2 // first 2 ctrees are in-memory

// Config is configuration for lsm tree
// nolint: maligned
type Config struct {
	Name string
	ID   uint32 // unique ID of LSM tree to verify that pages from correct tree are read
	Io   aio.LsmIo

	// 2 comparators as in rbtree to compare 2 items and item vs. key
	Comparator        rbtree.Comparator
	ComparatorWithKey rbtree.Comparator
	Comparator2Keys   rbtree.Comparator

	// On lsm merges or inserts to C0 we have to produce resulting item from pair (fresh item, old item).
	// By default result item = fresh item, but behaviour can be customizable by this cb (used for bitLsm).
	// SHOULD be idempotent and associative.
	MergeActionCb rbtree.ReplaceAction

	CreateEntry    EntryCreateFunc
	CreateEntryKey EntryKeyCreateFunc

	// node cache
	Cache cache.BasicCache

	// used to generate new lsm and ctree disk generations
	gens lsmGenerationSupplier

	// this non-public func may be used to call from lsm to tell externals
	// that new ctree created after merge (used for versioned lsm)
	onCtreeCreated func(*ctree) error
	// this non-public func may be used to call from lsm for freeing ctree blocks
	// after merge (used for versioned lsm)
	freeCtreeBlocks func(aio.CtreeID, []aio.CTreeNodeBlock) error

	NoThrottlingOnMerge bool

	// enables bloom filter for some first disk ctrees. Filter are constructed to reduce number of io reads in case item is not in tree.
	// Small bloom filter are saved to lsm desc, bigger - to the end of ctreeID object.
	// Bloom filters are not constructed for ctrees, that potentially may contain more than bloomSupportedMaxCtreeCount items.
	EnableBloomFilter bool

	Log *logrus.Logger
}

// Lsm is log-structured-merge tree implementation
// nolint: maligned
type Lsm struct {
	sync.RWMutex

	io     aio.LsmIo
	err    atomic.Value // fatal error state if happens on write
	cancel bool         // TODO: do we need this?

	// C0/C1 is a shortcut to Ctree[0] and Ctree[1] to avoid race detector thinking it can change where access
	// plus to avoid boundary checks on hot paths
	C0, C1        *ctree   `json:"-"`
	Ctree         []*ctree `json:"ctree"`
	Generation    uint64   `json:"-"` // generation of whole tree, incremented on each insert/remove + ctree merges
	nextDiskGen   uint32   // next Ctree DiskGeneration
	mergeCheckIdx int      // last checked tree for merge
	mergeDone     sync.Cond
	mergeDisable  int

	// same as in lsm config
	gens lsmGenerationSupplier

	cfg Config
}

type lsmGenerationSupplier interface {
	nextLsmGen() uint64
	nextCTreeDiskGen(idx int) uint64
}

// defaultGenSupplier used in lsm by default to calculate new generation counters
type defaultGenSupplier struct {
	lsm *Lsm
}

func (g *defaultGenSupplier) nextLsmGen() uint64 {
	return g.lsm.Generation + 1
}

func (g *defaultGenSupplier) nextCTreeDiskGen(idx int) uint64 {
	return uint64(idx)<<32 + uint64(atomic.AddUint32(&g.lsm.nextDiskGen, 1))
}

// New make Lsm struct from given config
func New(config Config) *Lsm {
	lsm := &Lsm{cfg: config, io: config.Io, mergeDisable: 0}
	lsm.gens = lsm.cfg.gens
	if lsm.gens == nil {
		lsm.gens = &defaultGenSupplier{lsm: lsm}
	}
	lsm.mergeDone.L = lsm
	if lsm.cfg.onCtreeCreated == nil {
		lsm.cfg.onCtreeCreated = func(*ctree) error { return nil }
	}
	if lsm.cfg.freeCtreeBlocks == nil {
		lsm.cfg.freeCtreeBlocks = func(id aio.CtreeID, blocks []aio.CTreeNodeBlock) error {
			return lsm.io.FreeBlocks(blocks)
		}
	}

	for i := 0; i < memLsmCtrees; i++ {
		lsm.addCtree()
	}
	lsm.C0 = lsm.Ctree[0]
	lsm.C1 = lsm.Ctree[1]

	return lsm
}

// should be called under lsm.Lock()
func (lsm *Lsm) addCtree() {
	assert(len(lsm.Ctree) < maxLsmCtrees)
	lsm.Ctree = append(lsm.Ctree, ctreeNew(lsm, len(lsm.Ctree)))
	lsm.Generation = lsm.gens.nextLsmGen()
}

// Insert given entry to tree
func (lsm *Lsm) Insert(item Entry) (err error) {
	if err = lsm.Error(); err != nil {
		return
	}

	lsm.Lock()

	C0, C1 := lsm.C0, lsm.C1
	C0.Insert(item)
	C0.Generation++
	lsm.Generation = lsm.gens.nextLsmGen()

	lsm.mergeCheck()

	for !lsm.cfg.NoThrottlingOnMerge && C1.merging && atomic.LoadInt64(&C0.Stats.LeafsSize) > C0.mergeSizeThreshold() {
		lsm.mergeDone.Wait()
	}

	lsm.Unlock()

	return nil
}

// Remove given entry from tree
func (lsm *Lsm) Remove(item Entry) (err error) {
	item.SetLsmDeleted(true)
	return lsm.Insert(item)
}

// MemUsage returns tree memory usage
func (lsm *Lsm) MemUsage() int {
	return int(atomic.LoadInt64(&lsm.C0.Stats.LeafsSize))
}

// Count returns total number of entries in all ctrees, including delete markers
func (lsm *Lsm) Count() (res int64) {
	for _, ctree := range lsm.Ctree {
		res += atomic.LoadInt64(&ctree.Stats.Count)
	}
	return
}

// Deleted returns total number of delete markers in all ctrees
func (lsm *Lsm) Deleted() (res int64) {
	for _, ctree := range lsm.Ctree {
		res += atomic.LoadInt64(&ctree.Stats.Deleted)
	}
	return
}

// MergeEnable decrements merge disable level
func (lsm *Lsm) MergeEnable() {
	lsm.Lock()
	lsm.mergeDisable--
	lsm.Unlock()
}

// MergeDisable increments merge disable level
func (lsm *Lsm) MergeDisable() {
	lsm.Lock()
	lsm.mergeDisable++
	lsm.Unlock()
}

func (lsm *Lsm) mergeCheck() {
	lsm.mergeCheckIdx--
	if lsm.mergeCheckIdx < 0 {
		lsm.mergeCheckIdx = len(lsm.Ctree) - 1
	}

	ctree := lsm.Ctree[lsm.mergeCheckIdx]
	if ctree.mergeRequired() && lsm.mergeDisable == 0 {
		lsm.mergeStart(ctree.Idx, ctree.Idx+1)
	}
}

func (lsm *Lsm) encodeC0Entries() (buf *SerializeBuf) {
	c0 := lsm.C0
	iter := c0.getRoot().items.First()
	if iter.Empty() {
		return &SerializeBuf{}
	}

	hdr := c0.encodeEntries(iter, 1024*1024*1024) // 1GB should never happen, so selected to write everything in single hdr
	buf, err := c0.encodeHdr(hdr, false)
	fatalOnErr(err)
	assert(iter.Empty()) // impossible that we failed to pack everything in 1GB...
	return
}

func (lsm *Lsm) decodeC0Entries(buf *DeserializeBuf) (err error) {
	c0 := lsm.C0
	hdr, err := c0.decodeHdr(buf)
	if err != nil {
		return errors.Wrap(err, "Invalid tree descriptor entries")
	}
	items, memSize, itemsSize, err := c0.decodeEntries(hdr)
	if err != nil {
		return errors.Wrap(err, "Invalid tree descriptor entries#2")
	}
	c0.getRoot().items = items
	c0.getRoot().memSize = memSize
	c0.getRoot().stateCsum = stateCsum(items)
	c0.Stats.LeafsSize = int64(itemsSize) // can't use len(buf) as it may compress very very well
	c0.Stats.Count = int64(items.Count())
	c0.Stats.Deleted = int64(hdr.deleted)
	return
}

// WriteDesc write lsm tree head descriptor with pointers to all ctrees
func (lsm *Lsm) WriteDesc(buf *SerializeBuf) {
	lsm.RLock()
	lsm.writeDescLocked(buf)
	lsm.RUnlock()
}

func (lsm *Lsm) writeDescLocked(buf *SerializeBuf) {
	assert(!lsm.isMergeInProgress()) // in case we forgot to wait merge done

	buf.EncodeFixedUint32(lsmMagic)
	buf.EncodeFixedUint32(lsmVersion)
	buf.EncodeFixedUint32(uint32(len(lsm.Ctree) - memLsmCtrees))
	buf.EncodeFixedUint32(lsm.nextDiskGen)

	for i := memLsmCtrees; i < len(lsm.Ctree); i++ {
		ctree := lsm.Ctree[i]
		buf.EncodeFixedUint64(ctree.DiskGeneration)
		buf.EncodeFixedInt64(ctree.RootOffs)
		buf.EncodeFixedInt64(ctree.RootSize)
		buf.EncodeFixedInt64(ctree.Stats.LeafsSize)
		buf.EncodeFixedInt64(ctree.Stats.DirsSize)
		buf.EncodeFixedInt64(ctree.Stats.Count)
		buf.EncodeFixedInt64(ctree.Stats.Deleted)
		ctree.writeBloomDesc(buf)
	}

	// save the rest of C0 tree left in-memory to LSM descriptor
	ebuf := lsm.encodeC0Entries()
	buf.EncodeSubSection(ebuf)

	VerifyDecoded(lsm.C0.getRoot())
}

// ReadDesc reads lsm tree head descriptor with pointers to all ctrees
func (lsm *Lsm) ReadDesc(dbuf *DeserializeBuf) (err error) {
	lsm.Lock()
	defer lsm.Unlock()

	magic := dbuf.DecodeFixedUint32()
	if magic != lsmMagic {
		return errors.New("Invalid tree descriptor magic")
	}
	version := dbuf.DecodeFixedUint32()
	_ = version
	roots := dbuf.DecodeFixedUint32()
	lsm.nextDiskGen = dbuf.DecodeFixedUint32()

	if roots > maxLsmCtrees {
		return errors.New("Too many tree roots, impossible normally?")
	}

	for i := 0; i < int(roots); i++ {
		ctree := &ctree{Idx: memLsmCtrees + i, lsm: lsm}
		ctree.DiskGeneration = dbuf.DecodeFixedUint64()
		ctree.RootOffs = dbuf.DecodeFixedInt64()
		ctree.RootSize = dbuf.DecodeFixedInt64()
		ctree.Stats.LeafsSize = dbuf.DecodeFixedInt64()
		ctree.Stats.DirsSize = dbuf.DecodeFixedInt64()
		ctree.Stats.Count = dbuf.DecodeFixedInt64()
		ctree.Stats.Deleted = dbuf.DecodeFixedInt64()
		if err = ctree.readBloomDesc(dbuf); err != nil {
			return err
		}
		lsm.Ctree = append(lsm.Ctree, ctree)
	}
	if err = dbuf.Error(); err != nil {
		return err
	}

	// restore C0 tree

	entriesBuf := dbuf.DecodeSubSection()
	if err = dbuf.Error(); err != nil {
		return errors.Wrap(err, "Invalid tree descriptor c0 section")
	}

	if entriesBuf.Len() != 0 {
		err = lsm.decodeC0Entries(entriesBuf)
		if err != nil {
			return errors.Wrap(err, "Invalid tree descriptor c0 content")
		}
	}

	return
}

// Flush memory contents
func (lsm *Lsm) Flush() error {
	return lsm.Merge(0, 1)
}

// Merge starts merge from given ctree to next one
func (lsm *Lsm) Merge(from, to int) error {
	lsm.Lock()

	lsm.waitMergeDoneLocked(from, to)
	lsm.mergeStart(from, to)
	lsm.waitMergeDoneLocked(from, to)

	lsm.Unlock()

	return lsm.Error()
}

// MergeAll starts sequential merge for all ctrees
func (lsm *Lsm) MergeAll() error {
	return lsm.Merge(0, len(lsm.Ctree)-1)
}

// Close does lsm closing checks
func (lsm *Lsm) Close() {
	for i := 0; i < len(lsm.Ctree); i++ {
		assert(!lsm.Ctree[i].merging)
	}
	if lsm.Error() == nil {
		// can have smth in Ctree[0] (saved to head), but not in Ctree[1] (merge in progress)
		assert(lsm.C1.getRoot() == nil || lsm.C1.getRoot().items.Empty())
	}
}

func (lsm *Lsm) setError(err error) {
	if err == nil || lsm.err.Load() != nil {
		return
	}
	lsm.err.Store(err)
}

// Error returns lsm error
func (lsm *Lsm) Error() (err error) {
	v := lsm.err.Load()
	if v != nil {
		err = v.(error)
	}
	return
}

// Dump prints lsm entries in GE order
func (lsm *Lsm) Dump() {
	for it := lsm.Find(GE, nil); !it.Empty(); it.Next() {
		fmt.Println(it.Result)
	}
}

// Name simply returns lsm name, with that it was initialized
func (lsm *Lsm) Name() string {
	return lsm.cfg.Name
}

// CurCtreeObjectNames returns array of object names,
// which correcpond to most recent ctrees states, which
// describe current lsm state
func (lsm *Lsm) CurCtreeObjectNames() []string {
	names := make([]string, len(lsm.Ctree))
	for i := range lsm.Ctree {
		names[i] = aio.GetCtreeObjectName("", lsm.Name(), lsm.Ctree[i].getCurrentID())
	}
	return names
}

func (lsm *Lsm) getCtreeDiskGenerations() (gens []uint64) {
	for _, c := range lsm.Ctree[memLsmCtrees:] {
		if c.DiskGeneration == 0 {
			continue
		}
		gens = append(gens, c.DiskGeneration)
	}
	return
}
