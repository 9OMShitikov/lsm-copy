package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"
	"unsafe"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/rbtree"
	SortedArray "github.com/neganovalexey/search/sarray"
	"github.com/pkg/errors"
)

const maxEntrySize = 1024 * 1024

const nodeHdrMagic uint32 = 0xdeaf3b92
const nodeVersion uint32 = 1

// single ctree node (with all items decoded)
// if it's directory node, then items = []dirEntry, otherwise contains user entries
type ctreeNode struct {
	items     rbtree.OrderedTree
	offs      int64
	size      int64
	memSize   int
	stateCsum uint32 // debug: immutable state verification checksum
	isDir     bool
}

var errNodeIsNotCached = errors.New("node is not cached")

type cachedCtreeNode struct {
	sync.Mutex
	node *ctreeNode
	buf  []byte // empty array in case ctreeNode is decoded
}

func (cachedNode *cachedCtreeNode) OnCacheDropped() {
	cachedNode.Lock()
	if cachedNode.node != nil {
		VerifyDecoded(cachedNode.node)
	}
	cachedNode.Unlock()
}

// description of single ctree node stored on disk disk
type ctreeNodeHdr struct {
	items      []byte // encoded items
	LastKey    EntryKey
	isDir      bool
	crc        uint32
	version    uint32
	lsmid      uint32
	generation uint64
	len, clen  uint32
	count      uint32
	deleted    uint32
	delmask    []bool
	memSize    int
}

type dirEntry struct {
	LastKey EntryKey
	keysCmp rbtree.Comparator
	offs    int64
	size    int64
}

var crcTab *crc32.Table = crc32.MakeTable(crc32.Castagnoli)

// getMemoryCleanEntryKey returns entry key which doesn't hold memory references to foreign buffers
// Explanation:
// Entry and EntryKey may refer to decompressed LSM node buffer as DecodeStr doesn't copy by default
// As a result, merge routine can refer to lots of node buffers via last key element which is tracked
// to write out directory pages later.
// To avoid this we have to serialize and deserialize the entry key.
func (ctree *ctree) getMemoryCleanEntryKey(entry Entry) EntryKey {
	buf := NewSerializeBuf(128)
	entry.GetKey().Encode(buf)

	dbuf := NewDeserializeBuf(buf.Bytes())
	dbuf.Ver = nodeVersion

	keyCopy := ctree.lsm.cfg.CreateEntryKey()
	err := keyCopy.Decode(dbuf)
	fatalOnErr(err)
	return keyCopy
}

// encode user entries into buffer (w/o node header)
func (ctree *ctree) encodeEntries(entries rbtree.OrderedIter, targetSize int) (hdr *ctreeNodeHdr) {
	buf := NewSerializeBuf(minInt(targetSize+1024, 256*1024))
	delmask := make([]bool, 0, 256)
	var count, deleted uint32
	var entry, prevEntry Entry
	memSize := 0
	entries.ForEach(func(i interface{}) bool {
		// stop if target size reached already and at least 4 entries were saved
		if buf.Len() >= targetSize && count >= 4 {
			return false
		}

		prevEntry, entry = entry, i.(Entry)
		entry.Encode(buf, prevEntry)
		del := entry.LsmDeleted()
		delmask = append(delmask, del)
		if del {
			deleted++
		}
		memSize += entry.Size() + 16 // 16 for tree sarray element
		count++

		return true
	})

	lastKey := ctree.getMemoryCleanEntryKey(entry)
	hdr = &ctreeNodeHdr{isDir: false, items: buf.Bytes(), LastKey: lastKey, delmask: delmask, count: count, deleted: deleted, memSize: memSize}
	return
}

// decode user entries from the buffer (w/o node header)
func (ctree *ctree) decodeEntries(hdr *ctreeNodeHdr) (entries rbtree.OrderedTree, memSize, itemsSize int, err error) {
	entryOverhead := 0
	lsm := ctree.lsm
	if ctree.Idx < memLsmCtrees {
		entries = rbtree.New(lsm.cfg.Comparator, lsm.cfg.ComparatorWithKey)
		memSize += int(unsafe.Sizeof(rbtree.Tree{}))
		entryOverhead = 8 + int(unsafe.Sizeof(rbtree.Node{})) // ptr to node + node struct
	} else {
		entries = SortedArray.New(lsm.cfg.Comparator, lsm.cfg.ComparatorWithKey).Prealloc(int(hdr.count))
		memSize += int(unsafe.Sizeof(SortedArray.Array{}))
		entryOverhead = 16 // each element stored as an interface (two pointers)
	}
	memSize += entryOverhead * int(hdr.count)

	dbuf := NewDeserializeBuf(hdr.items)
	dbuf.Ver = hdr.version

	var e, prevEntry Entry
	for n := uint32(0); dbuf.Len() > 0; n++ {
		if n >= hdr.count {
			err = errors.New("found more entries in node then expected")
			return
		}
		prevEntry, e = e, lsm.cfg.CreateEntry()
		err = e.Decode(dbuf, prevEntry)
		if err != nil {
			return
		}
		if hdr.delmask[n] {
			e.SetLsmDeleted(true)
			hdr.deleted++
		}
		entries.Insert(e)
		itemsSize += e.Size()
	}
	if entries.Count() != int(hdr.count) {
		err = errors.New("found less entries in node then expected")
		return
	}
	memSize += itemsSize
	return
}

// encode directory entries to the buffer (w/o node header)
func (ctree *ctree) encodeDirs(dirs *[]dirEntry, targetSize int) (hdr *ctreeNodeHdr) {
	buf := NewSerializeBuf(targetSize + 1024)
	var count uint32
	var dir dirEntry

	for _, dir = range *dirs {
		dir.LastKey.Encode(buf)
		buf.EncodeInt64(dir.offs / io.PageSize)
		buf.EncodeInt64(dir.size / io.PageSize)
		count++

		// stop if target size reached already and at least 4 dirs were saved
		if buf.Len() > targetSize && count >= 4 {
			break
		}
	}
	*dirs = (*dirs)[count:]
	memSize := 64*int(count) + buf.Len() // see decodeDirs for explanation of 64
	hdr = &ctreeNodeHdr{isDir: true, items: buf.Bytes(), LastKey: dir.LastKey, delmask: nil, count: count, clen: uint32(buf.Len()), len: uint32(buf.Len()), memSize: memSize}
	return
}

func dirEntryCmpWithKey(entry, key interface{}) int {
	dir := entry.(*dirEntry)
	ekey := dir.LastKey
	return dir.keysCmp(ekey, key)
}

// decode directory entries from the buffer (w/o node header)
func (ctree *ctree) decodeDirs(hdr *ctreeNodeHdr) (dirs rbtree.OrderedTree, memSize int, err error) {
	lsm := ctree.lsm

	buf := NewDeserializeBuf(hdr.items)
	buf.Ver = hdr.version

	dirs = SortedArray.New(nil, dirEntryCmpWithKey).Prealloc(int(hdr.count))

	// each sArray element has an overhead of interface{} size
	memSize = int(unsafe.Sizeof(SortedArray.Array{})) + int(hdr.count)*16

	for buf.Len() > 0 {
		e := lsm.cfg.CreateEntryKey()
		err = e.Decode(buf)

		dir := &dirEntry{LastKey: e, keysCmp: lsm.cfg.Comparator2Keys}
		dir.offs = buf.DecodeInt64()
		dir.offs *= io.PageSize
		dir.size = buf.DecodeInt64()
		dir.size *= io.PageSize

		err = FirstErr(err, buf.Error())
		if err != nil {
			return nil, 0, err
		}

		dirs.Insert(dir)

		// dir entry + last key size
		memSize += int(unsafe.Sizeof(dirEntry{})) + e.Size()
	}
	if dirs.Count() != int(hdr.count) {
		return nil, 0, errors.New("dirs count mismatch in lsm node")
	}

	return
}

// encode delete mask to mark which entries were deleted
func (hdr *ctreeNodeHdr) encodeDelmask(buf *SerializeBuf) {
	if hdr.isDir {
		return
	}

	var i, n, bit uint32
	for i = 0; i < (hdr.count+63)/64; i++ {
		var v uint64
		for bit = 0; bit < 64 && n < hdr.count; bit, n = bit+1, n+1 {
			if hdr.delmask[int(n)] {
				v = v | (1 << bit)
			}
		}
		buf.EncodeFixedUint64(v)
	}
}

// decode delete mask to recover deleted state of entries
func (hdr *ctreeNodeHdr) decodeDelmask(buf *DeserializeBuf) {
	if hdr.isDir {
		return
	}

	hdr.delmask = make([]bool, 0, hdr.count)
	var i, n, bit uint32
	for i = 0; i < (hdr.count+63)/64; i++ {
		v := buf.DecodeFixedUint64()
		for bit = 0; bit < 64 && n < hdr.count; bit, n = bit+1, n+1 {
			del := (v & (1 << bit)) != 0
			hdr.delmask = append(hdr.delmask, del)
		}
	}
}

// encode ctree node header to the buffer
func (ctree *ctree) encodeHdr(hdr *ctreeNodeHdr, pagePadding bool) (buf *SerializeBuf, err error) {
	hdr.len = uint32(len(hdr.items))
	hdr.clen = hdr.len

	// assemble header in buffer
	buf = NewSerializeBuf(64 + len(hdr.items) + io.PageSize)
	buf.EncodeFixedUint32(nodeHdrMagic)
	crcOffs := 4
	buf.EncodeFixedUint32(0) // placeholder for crc
	buf.EncodeBool(hdr.isDir)
	buf.EncodeFixedUint32(ctree.lsm.cfg.ID)
	buf.EncodeFixedUint64(ctree.mergeDiskGen) // writing future version, switching to it in setupRoot()
	buf.EncodeUint32(nodeVersion)
	buf.EncodeFixedUint32(hdr.count)
	buf.EncodeFixedUint32(hdr.len)
	buf.EncodeFixedUint32(hdr.clen)
	hdr.encodeDelmask(buf)
	buf.EncodeFixedUint32(nodeHdrMagic)

	// return hdr + items
	buf.WriteRaw(hdr.items)
	hdr.items = nil // release memory

	if pagePadding {
		// pad to page boundary
		buf.AlignTo(io.PageSize)
	}

	// calculate checksum
	crc := crc32.Checksum(buf.Bytes(), crcTab)
	binary.BigEndian.PutUint32(buf.Bytes()[crcOffs:], crc)

	return buf, nil
}

// decode ctree node header from the buffer
func (ctree *ctree) decodeHdr(buf0 *DeserializeBuf) (hdr *ctreeNodeHdr, err error) {
	bufChangeable := *buf0
	buf := &bufChangeable
	hdr = &ctreeNodeHdr{}

	magic1 := buf.DecodeFixedUint32()
	if magic1 != nodeHdrMagic {
		return nil, FirstErr(buf.Error(), fmt.Errorf("Tree node header magic mismatch %x", magic1))
	}

	crcBuf := buf.Bytes()
	hdr.crc = buf.DecodeFixedUint32()
	binary.BigEndian.PutUint32(crcBuf, 0)
	crc := crc32.Checksum(buf0.Bytes(), crcTab)
	binary.BigEndian.PutUint32(crcBuf, hdr.crc)
	if hdr.crc != crc {
		return nil, FirstErr(buf.Error(), fmt.Errorf("Tree node checksum mismatch %x %x", hdr.crc, crc))
	}

	hdr.isDir = buf.DecodeBool()
	hdr.lsmid = buf.DecodeFixedUint32()
	if hdr.lsmid != ctree.lsm.cfg.ID {
		return nil, FirstErr(buf.Error(), fmt.Errorf("Tree header id mismatch %v, exp %v", hdr.lsmid, ctree.lsm.cfg.ID))
	}
	hdr.generation = buf.DecodeFixedUint64()
	hdr.version = buf.DecodeUint32()
	if hdr.version > nodeVersion {
		return nil, FirstErr(buf.Error(), errors.New("Tree format v"+strconv.Itoa(int(hdr.version))+" is newer then supported"))
	}
	hdr.count = buf.DecodeFixedUint32()
	hdr.len = buf.DecodeFixedUint32()
	hdr.clen = buf.DecodeFixedUint32()
	hdr.decodeDelmask(buf)
	magic2 := buf.DecodeFixedUint32()
	if err = buf.Error(); err != nil {
		return nil, errors.New("Tree header decoding error: " + err.Error())
	}
	if magic2 != nodeHdrMagic {
		return nil, errors.New("Tree header magic2 mismatch")
	}
	if hdr.generation != ctree.DiskGeneration && ctree.Idx >= memLsmCtrees {
		return nil, fmt.Errorf("Tree header generation mismatch (%v %v)", hdr.generation, ctree.DiskGeneration)
	}

	hdr.items = buf.Bytes()
	hdr.items = hdr.items[0:hdr.clen] // remove padding

	return
}

func adjustTargetSize(clen int, curTargetSize int) int {
	ztargetSize := 16384
	maxTargetSize := 65536

	if curTargetSize == 0 {
		return ztargetSize
	}

	// adjust targetSize to achieve better approx of ztargetSize
	if clen < ztargetSize*9/10 && curTargetSize < maxTargetSize {
		return curTargetSize * 5 / 4
	}
	if clen > ztargetSize-512 && curTargetSize > ztargetSize {
		return curTargetSize * 9 / 10
	}
	return curTargetSize
}

// write LSM entries first, collecting page offsets for dir entries
func (ctree *ctree) writeEntries(items rbtree.OrderedTree, stats *ctreeStats, writer io.MergeWriter) (dirs []dirEntry, err error) {
	targetSize := adjustTargetSize(0, 0)
	dirs = make([]dirEntry, 0, 256)

	for iter := items.First(); !iter.Empty(); {
		var buf *SerializeBuf
		hdr := ctree.encodeEntries(iter, targetSize)
		if buf, err = ctree.encodeHdr(hdr, true); err != nil {
			return
		}

		dir := dirEntry{LastKey: hdr.LastKey, size: int64(buf.Len())}
		dir.offs, err = writer.WritePages(buf.Bytes())
		if err != nil {
			return nil, err
		}
		dirs = append(dirs, dir)

		stats.LeafsSize += dir.size
		stats.Count += int64(hdr.count)
		stats.Deleted += int64(hdr.deleted)

		targetSize = adjustTargetSize(int(hdr.clen), targetSize)

		if ctree.Idx <= 2 { // it's useless to cache larger ctrees as they don't fit
			// adding not decoded item on write
			cachedObj := &cachedCtreeNode{buf: buf.Bytes()}
			// put node in cache with identifier of next ctree, which will be valid after merge completion
			ctree.lsm.cfg.Cache.Write(ctree.getMergeID(), dir.offs, cachedObj, int64(hdr.memSize))
		}
	}
	return dirs, nil
}

// write dir entries, collecting page offsets for higher-level dir entries
func (ctree *ctree) writeDirs(dirs []dirEntry, stats *ctreeStats, writer io.MergeWriter) (outdirs []dirEntry, err error) {
	targetSize := adjustTargetSize(0, 0)
	odirs := make([]dirEntry, 0, 256)

	for len(dirs) > 0 {
		var buf *SerializeBuf
		hdr := ctree.encodeDirs(&dirs, targetSize)

		if buf, err = ctree.encodeHdr(hdr, true); err != nil {
			return
		}

		dir := dirEntry{LastKey: hdr.LastKey, size: int64(buf.Len())}
		dir.offs, err = writer.WritePages(buf.Bytes())
		if err != nil {
			return nil, err
		}
		odirs = append(odirs, dir)
		stats.DirsSize += dir.size

		targetSize = adjustTargetSize(int(hdr.clen), targetSize)

		if ctree.Idx <= 2 { // it's useless to cache larger ctrees as they don't fit
			// adding not decoded dir items on write
			cachedObj := &cachedCtreeNode{buf: buf.Bytes()}
			// remember that written at merge nodes should go under name, specified by merge writer
			ctree.lsm.cfg.Cache.Write(ctree.getMergeID(), dir.offs, cachedObj, int64(hdr.memSize))
		}
	}
	return odirs, nil
}

// readNode first looks for node in cache and in case of failure fallbacks
// to reading from disk (readNodeNoCache)
// NOTE: canBlock = false means that only cached reads are allowed, not synchronous I/O
func (ctree *ctree) readNode(offs, size int64, canBlock bool) (*ctreeNode, error) {
	nodeCache := ctree.lsm.cfg.Cache

	// reads are always try to read nodes with current name
	// nolint: scopelint
	obj, err := nodeCache.Read(ctree.getCurrentID(), offs, func() (cache.CachedObj, int64, error) {
		if !canBlock {
			return nil, 0, errNodeIsNotCached
		}
		node, err := ctree.readNodeNoCache(offs, size)
		if err != nil {
			return nil, 0, err
		}

		cachedNode := &cachedCtreeNode{buf: nil, node: node}
		return cachedNode, int64(node.memSize), nil
	})

	if err != nil {
		return nil, err
	}

	cachedNode := obj.(*cachedCtreeNode)
	cachedNode.Lock()
	defer cachedNode.Unlock()
	if cachedNode.buf != nil {
		// decoding
		var decodedNode *ctreeNode
		decodedNode, err = ctree.decodeNode(cachedNode.buf, offs, size)
		if err != nil {
			return nil, err
		}
		cachedNode.node = decodedNode
		cachedNode.buf = nil
	}

	assert(cachedNode.node.size == size)

	return cachedNode.node, nil
}

func (ctree *ctree) decodeNode(buf []byte, offs, size int64) (*ctreeNode, error) {
	hdr, err := ctree.decodeHdr(NewDeserializeBuf(buf))
	if err != nil {
		return nil, err
	}

	node := &ctreeNode{isDir: hdr.isDir, offs: offs, size: size}
	if hdr.isDir {
		node.items, node.memSize, err = ctree.decodeDirs(hdr)
	} else {
		node.items, node.memSize, _, err = ctree.decodeEntries(hdr)
		node.stateCsum = stateCsum(node.items)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to decode tree node at offs=%v+%v", offs, size)
	}

	return node, nil
}

// reads single ctree node (can span multiple pages)
func (ctree *ctree) readNodeNoCache(offs, size int64) (node *ctreeNode, err error) {
	bs, err := ctree.lsm.io.ReadPages(ctree.getCurrentID(), offs, size)
	if err != nil {
		return nil, errors.Wrapf(err, "[LSM = %s] Failed to read tree (%v) node at offs=%v+%v",
			ctree.lsm.Name(), ctree.getCurrentID(), offs, size)
	}

	node, err = ctree.decodeNode(bs, offs, size)
	if err != nil {
		return nil, errors.Wrapf(err, "[LSM = %s] Failed to decode node (tree %v) at offs=%v+%v",
			ctree.lsm.Name(), ctree.getCurrentID(), offs, size)
	}

	return
}
