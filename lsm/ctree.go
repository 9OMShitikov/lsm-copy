package lsm

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/rbtree"
	"github.com/pkg/errors"
)

type ctree struct {
	Idx int `json:"idx"` // Ctree index in LSM tree

	merging      bool   // used in merge at the moment
	mergeDiskGen uint64 // next DiskGeneration value assigned after merge is finished

	Generation     uint64 `json:"-"`   // generation of ctree in mem, changes every time when ctree is modified
	DiskGeneration uint64 `json:"gen"` // generation of ctree, changes every time when ctree is modified

	RootOffs int64        `json:"root_offs"`
	RootSize int64        `json:"root_size"`
	root     atomic.Value // ptr to ctreeNode
	Stats    ctreeStats   `json:"stats"`

	// bloom filter for this ctree (nil means no bloom filter is used or not read yet).
	// Used only for disk trees. This filter is updated only during merge
	bloom                   atomic.Value
	bloomLoadAfterMissCount int64 // if bloom stores with ctree (not in lsm desc) => bloom will read after this counter become 0
	BloomOffs               int64 `json:"bloom_offs"`
	BloomSize               int64 `json:"bloom_size"`
	bloomReadMtx            sync.Mutex

	lsm *Lsm
}

// total size and counter of entries written to ctree
type ctreeStats struct {
	LeafsSize int64 `json:"leafs_size"`
	DirsSize  int64 `json:"dirs_size"`
	Count     int64 `json:"count"`
	Deleted   int64 `json:"deleted"`
}

const (
	//EQ is for "=="
	EQ = rbtree.EQ
	//GE is for ">="
	GE = rbtree.GE
	//GT is for ">"
	GT = rbtree.GT
	//LE is for "<="
	LE = rbtree.LE
	//LT is for "<"
	LT = rbtree.LT
)

type ctreeIter struct {
	op     int
	ctree  *ctree
	key    EntryKey
	keyMax EntryKey // key up to which search is performed

	Generation uint64               // ctree.Generation when started
	nodes      []*ctreeNode         // chain of lookup nodes, [0] = root
	iters      []rbtree.OrderedIter // chain of node iterators: iters[i] correspond to nodes[i]
	result     rbtree.OrderedIter   // list of entries in the leaf node

	nodeBlocks  []io.CTreeNodeBlock // disk extents used by this iterator during lookups
	trackBlocks bool
}

func rbtreeNew(lsm *Lsm) *rbtree.Tree {
   return rbtree.NewWithReplaceAction(lsm.cfg.Comparator, lsm.cfg.ComparatorWithKey, lsm.cfg.MergeActionCb)
}

func ctreeNew(lsm *Lsm, idx int) (ct *ctree) {
	ct = &ctree{lsm: lsm, Idx: idx}
	if idx == 0 {
		ct.setRoot(&ctreeNode{items: rbtreeNew(lsm)})
	}
	return
}

func (ctree *ctree) Insert(item Entry) {
	root := ctree.getRoot()
	old, result := root.items.(*rbtree.Tree).InsertOrReplace(item, true)
	if item.LsmDeleted() {
		atomic.AddInt64(&ctree.Stats.Deleted, 1)
	}
	if old == nil {
		atomic.AddInt64(&ctree.Stats.Count, 1)
	} else {
		oldEntry := old.(Entry)
		atomic.AddInt64(&ctree.Stats.LeafsSize, -int64(oldEntry.Size()))
		if oldEntry.LsmDeleted() {
			d := atomic.AddInt64(&ctree.Stats.Deleted, -1)
			assert(d >= 0)
		}
		stateRemove(&root.stateCsum, oldEntry)
	}
	resultEntry := result.(Entry)
	atomic.AddInt64(&ctree.Stats.LeafsSize, int64(resultEntry.Size()))
	stateAdd(&root.stateCsum, resultEntry)
}

func (ctree *ctree) getRoot() (root *ctreeNode) {
	v := ctree.root.Load()
	if v == nil {
		return
	}
	return v.(*ctreeNode)
}

func (ctree *ctree) setRoot(root *ctreeNode) {
	ctree.root.Store(root)
}

func (ctree *ctree) readRoot(canBlock bool) (root *ctreeNode, err error) {
	if root = ctree.getRoot(); root != nil {
		return root, nil
	}
	if ctree.Empty() {
		return nil, nil
	}

	root, err = ctree.readNode(ctree.RootOffs, ctree.RootSize, canBlock)
	ctree.setRoot(root)

	return
}

func (ctree *ctree) Empty() bool {
	if ctree.Idx < memLsmCtrees {
		root := ctree.getRoot()
		return root == nil || root.items.Empty()
	}
	return ctree.RootSize == 0
}

// getCurrentID returns current ctree identifier (used in cache to see the difference between ctrees and lsms)
func (ctree *ctree) getCurrentID() io.CtreeID {
	return io.CtreeID{LsmID: ctree.lsm.cfg.ID, DiskGen: ctree.DiskGeneration}
}

// getMergeID returns ctree cache id which will be valid after merge into that ctree is finished
// (with DiskGeneration incremented)
func (ctree *ctree) getMergeID() io.CtreeID {
	return io.CtreeID{LsmID: ctree.lsm.cfg.ID, DiskGen: ctree.mergeDiskGen}
}

func (ctree *ctree) setStats(st *ctreeStats) {
	atomic.StoreInt64(&ctree.Stats.LeafsSize, atomic.LoadInt64(&st.LeafsSize))
	atomic.StoreInt64(&ctree.Stats.DirsSize, atomic.LoadInt64(&st.DirsSize))
	atomic.StoreInt64(&ctree.Stats.Count, atomic.LoadInt64(&st.Count))
	atomic.StoreInt64(&ctree.Stats.Deleted, atomic.LoadInt64(&st.Deleted))
}

func (ctree *ctree) stealRoot(src *ctree) {
	lsm := ctree.lsm

	lsm.Generation = lsm.gens.nextLsmGen()
	ctree.Generation++
	src.Generation++
	ctree.DiskGeneration = src.DiskGeneration

	ctree.RootOffs, ctree.RootSize = src.RootOffs, src.RootSize
	src.RootOffs, src.RootSize, src.DiskGeneration = 0, 0, 0
	ctree.setRoot(src.getRoot())
	src.setRoot(nil)
	ctree.setStats(&src.Stats)
	src.setStats(&ctreeStats{})
	ctree.merging, src.merging = src.merging, false
	ctree.setupBloom(src.BloomOffs, src.BloomSize, src.getBloom())
	src.setupBloom(0, 0, nil)

	if src.Idx == 0 {
		src.setRoot(&ctreeNode{items: rbtreeNew(ctree.lsm)})
	}
}

func (ctree *ctree) setupRoot(offs int64, size int64, diskGen uint64, stats *ctreeStats) {
	lsm := ctree.lsm
	lsm.Generation = lsm.gens.nextLsmGen()
	ctree.Generation++
	ctree.DiskGeneration = diskGen

	ctree.RootOffs, ctree.RootSize = offs, size
	ctree.setStats(stats)
	ctree.setRoot(nil)
}

func (ctree *ctree) setupBloom(offs, size int64, bloom *ctreeBloom) {
	ctree.BloomOffs, ctree.BloomSize = offs, size
	ctree.bloom.Store(bloom)
}

func (ctree *ctree) hasBloom() bool {
	return ctree.getBloom() != nil || ctree.BloomOffs != 0
}

func (ctree *ctree) bloomEnabled() bool {
	return ctree.lsm.cfg.EnableBloomFilter && !ctree.Empty() && ctree.Stats.Count <= bloomSupportedMaxCtreeCount
}

func findOp(node *ctreeNode, op int) int {
	// EQ is special: while we look through dirs we have to lookup GE, and only for leaf for EQ
	if op == EQ && node.isDir {
		return GE
	}
	return op
}

func (it *ctreeIter) findNext(canBlock bool) (err error) {
	it.result = nil
	var node *ctreeNode
	var iter rbtree.OrderedIter
	for {
		node = it.nodes[len(it.nodes)-1]
		iter = it.iters[len(it.iters)-1]
		if iter.Empty() {
			return
		}

		it.ctree.lsm.debug2("findNext: ctree=%v:%v, item=%v", it.ctree.Idx, len(it.nodes)-1, iter.Item())
		if !node.isDir {
			// found result in leaf node (or not found)
			it.result = iter
			return
		}

		dir := iter.Item().(*dirEntry)
		node, err = it.ctree.readNode(dir.offs, dir.size, canBlock)
		if err != nil {
			return
		}

		// advancing directory node iterator so next time currently read node
		// will be skipped
		if it.op == GE || it.op == GT || it.op == EQ {
			iter.Next()
		} else {
			iter.Prev()
		}

		// adding new node to the stack
		it.pushNode(node)
	}
}

func (ctree *ctree) NewIter(op int, keyFrom EntryKey, keyMax EntryKey) (it ctreeIter) {
	it = ctreeIter{ctree: ctree, op: op, key: keyFrom, keyMax: keyMax, Generation: ctree.Generation}
	return
}

// Find initializes iterator, which iterates through elements `e` in ctree so:
//     if op == GT (GE, EQ) : keyFrom < (<=) Key(e) <= keyMax
//     if op == LT (LE)     : keyFrom > (>=) Key(e) >= keyMax
// If you don't want to use `keyMax` bound, pass nil value
func (it *ctreeIter) Find(canBlock bool) (err error) {
	if it.op == EQ { // check bloom filters firstly
		if err = it.ctree.readBloomIfNeeded(canBlock); err != nil {
			return
		}
		if !it.ctree.getBloom().exists(it.key) {
			return
		}
	}

	if len(it.nodes) == 0 {
		var root *ctreeNode
		root, err = it.ctree.readRoot(canBlock)
		if root == nil || err != nil {
			return
		}
		it.pushNode(root)
	}

	err = it.nextRelookup(canBlock)

	if err == nil {
		it.stopOutOfSearchRange()
	}

	if err == nil && it.op == EQ && it.result == nil {
		atomic.AddInt64(&it.ctree.bloomLoadAfterMissCount, -1)
	}

	return
}

func (it *ctreeIter) pushNode(node *ctreeNode) {
	it.nodes = append(it.nodes, node)
	nodeIter := node.items.Find(findOp(node, it.op), it.key)

	// ==== Special case for Find(LE/LT): ====
	//
	// Imagine ctree with dirs nodes and leafs like that:
	//                 A(max=200)
	//    B(max=50)    C(max=150)   D(max=200)
	//     20...50       70..150    155...200    [leafs]
	// `A.Find(LE, 100)` will return `B` node, but as we can see suitable keys may be found in `C` node,
	// which is next to `B`. Also you can see that only one node next to found is interesting because otherwise
	// it means that max key of "C" node is less than key, but when it would be found instead of "B"
	if (it.op == LE || it.op == LT) && node.isDir {
		if nodeIter.Empty() {
			nodeIter = node.items.First()
		} else if nodeIter.HasNext() {
			nodeIter.Next()
		}
	}

	it.iters = append(it.iters, nodeIter)
}

func (it *ctreeIter) trackNodeBlocks(node *ctreeNode) {
	if !it.trackBlocks || node.size == 0 /* mem ctree root */ {
		return
	}

	if len(it.nodeBlocks) > 0 {
		last := &it.nodeBlocks[len(it.nodeBlocks)-1]
		if last.Offs+last.Size == node.offs {
			last.Size += node.size
			return
		}
	}

	blk := io.CTreeNodeBlock{CtreeID: it.ctree.getCurrentID(), Offs: node.offs, Size: node.size}
	it.nodeBlocks = append(it.nodeBlocks, blk)
}

func (it *ctreeIter) up() {
	if len(it.nodes) > 0 {
		lastNode := it.nodes[len(it.nodes)-1]
		it.trackNodeBlocks(lastNode)
	}
	it.nodes = it.nodes[0 : len(it.nodes)-1]
	it.iters = it.iters[0 : len(it.iters)-1]
}

func (it *ctreeIter) nextRelookup(canBlock bool) (err error) {
	it.result = nil

	for len(it.nodes) > 0 {
		err = it.findNext(canBlock)
		if err != nil {
			return
		}

		// In case opeartion is EQ we should return anyway because
		// either result is not nil or there is no interesting item
		// in the tree => so we just returning
		if it.result != nil || it.op == EQ {
			it.up()
			return
		}

		it.up()
	}

	return
}

func (it *ctreeIter) Next() (err error) {
	if it.result == nil {
		return
	}

	assert(it.op != EQ)

	var found bool
	if it.op == GT || it.op == GE {
		found = it.result.Next()
	} else {
		found = it.result.Prev()
	}

	if !found {
		err = it.nextRelookup(true)
	}

	if err == nil {
		it.stopOutOfSearchRange()
	}
	return
}

func (it *ctreeIter) Empty() bool {
	return it.result == nil
}

func (it *ctreeIter) Item() interface{} {
	if it.result == nil {
		return nil
	}

	return it.result.Item()
}

func (it *ctreeIter) stopOutOfSearchRange() {
	if it.result == nil || it.result.Empty() || it.isEntryInSearchRange(it.result.Item()) {
		return // OK
	}
	it.result = nil
}

func (it *ctreeIter) isEntryInSearchRange(entry interface{}) bool {
	if it.keyMax == nil {
		return true
	}

	cmpRes := it.ctree.lsm.cfg.ComparatorWithKey(entry, it.keyMax)
	if (it.op == GT || it.op == GE || it.op == EQ) && cmpRes > 0 {
		// item is greater than right search bound
		return false
	}
	if (it.op == LT || it.op == LE) && cmpRes < 0 {
		// item is less than left search bound
		return false
	}
	return true
}

func (ctree *ctree) forEachNode(node *ctreeNode, cb func(*ctreeNode) error) (err error) {
	err = cb(node)
	if err != nil {
		return
	}

	if !node.isDir {
		return
	}
	var child *ctreeNode
	for _, item := range node.items.Items() {
		dir := item.(*dirEntry)
		child, err = ctree.readNode(dir.offs, dir.size, true)
		if err != nil {
			return
		}
		assert(child.offs == dir.offs)
		assert(child.size == dir.size)

		var lastKey EntryKey
		if child.isDir {
			lastKey = child.items.Last().Item().(*dirEntry).LastKey
		} else {
			lastKey = child.items.Last().Item().(Entry).GetKey()
		}
		if dir.keysCmp(dir.LastKey, lastKey) != 0 {
			return errors.New("dir entry last key doesn't match last element key")
		}

		err = ctree.forEachNode(child, cb)
		if err != nil {
			return
		}
	}
	return
}

func (ctree *ctree) ForEachNode(cb func(node *ctreeNode) error) (err error) {
	root, err := ctree.readRoot(true)
	if err != nil {
		return
	}
	if root == nil {
		return
	}

	return ctree.forEachNode(root, cb)
}

func (ctree *ctree) FreeBlocks() (err error) {
	it := ctreeIter{ctree: ctree, trackBlocks: true}
	err = ctree.ForEachNode(func(node *ctreeNode) error {
		it.trackNodeBlocks(node)
		return nil
	})
	if err != nil {
		return
	}
	nodeBlocks := it.nodeBlocks
	// add bloom filter range to free it too
	if ctree.BloomOffs != 0 {
		nodeBlocks = append(nodeBlocks, io.CTreeNodeBlock{CtreeID: ctree.getCurrentID(), Offs: ctree.BloomOffs, Size: ctree.BloomSize})
	}

	return ctree.lsm.io.FreeBlocks(nodeBlocks)
}

func (ctree *ctree) getBloom() (cbloom *ctreeBloom) {
	v := ctree.bloom.Load()
	if v == nil {
		return nil
	}
	return v.(*ctreeBloom)
}

func (ctree *ctree) readBloomIfNeeded(canBlock bool) (err error) {
	if ctree.getBloom() != nil || ctree.BloomOffs == 0 {
		return
	}
	if ctree.BloomSize > 128*1024 && atomic.LoadInt64(&ctree.bloomLoadAfterMissCount) > 0 {
		// small number of misses => bloom filter is not effective, so there is no need to read it
		return
	}
	if !canBlock {
		return errNodeIsNotCached
	}

	ctree.bloomReadMtx.Lock()
	defer ctree.bloomReadMtx.Unlock()

	// double check
	if ctree.getBloom() != nil {
		return
	}

	bs, err := ctree.lsm.io.ReadPages(ctree.getCurrentID(), ctree.BloomOffs, ctree.BloomSize)
	if err != nil {
		return errors.Wrapf(err, "[LSM = %s] Failed to read tree (%v) bloom at offs=%v+%v",
			ctree.lsm.Name(), ctree.getCurrentID(), ctree.BloomOffs, ctree.BloomSize)
	}

	cbloom, err := decodeCtreeBloom(NewDeserializeBuf(bs))
	if err != nil {
		return errors.Wrapf(err, "[LSM = %s] Failed to decode bloom (tree %v) at offs=%v+%v",
			ctree.lsm.Name(), ctree.getCurrentID(), ctree.BloomOffs, ctree.BloomSize)
	}
	ctree.bloom.Store(cbloom)
	return
}

func (ctree *ctree) writeBloomDesc(buf *SerializeBuf) {
	buf.EncodeFixedUint32(bloomHdrMagic)
	buf.EncodeUint32(bloomVersion)
	buf.EncodeInt64(ctree.BloomOffs)
	buf.EncodeInt64(ctree.BloomSize)
	cbuf := NewSerializeBuf(0)
	if bloom := ctree.getBloom(); bloom != nil && ctree.BloomOffs == 0 {
		// bloom holds in memory - need to save to desc
		cbuf = NewSerializeBuf(bloom.length() + 64)
		ctree.getBloom().encode(cbuf, false)
	}
	buf.EncodeSubSection(cbuf)
}

func (ctree *ctree) readBloomDesc(dbuf *DeserializeBuf) (err error) {
	magic := dbuf.DecodeFixedUint32()
	if magic != bloomHdrMagic {
		return FirstErr(dbuf.Error(), errors.Errorf("bloom: wrong magic %d", magic))
	}
	version := dbuf.DecodeUint32()
	if version > bloomVersion {
		return FirstErr(dbuf.Error(), errors.New("bloom format v"+strconv.Itoa(int(version))+" is newer then supported"))
	}
	ctree.BloomOffs = dbuf.DecodeInt64()
	ctree.BloomSize = dbuf.DecodeInt64()
	cbuf := dbuf.DecodeSubSection()
	if cbuf.Len() == 0 {
		return
	}
	cbloom, err := decodeCtreeBloom(cbuf)
	if err != nil {
		return
	}
	ctree.bloomLoadAfterMissCount = 32 // setup possible number of misses
	ctree.bloom.Store(cbloom)
	return
}
