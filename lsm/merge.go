package lsm

import (
	"runtime"
	"sync/atomic"

	"github.com/neganovalexey/search/io"
	SortedArray "github.com/neganovalexey/search/sarray"
)

// defaults for merge-configuring values
const (
	defaultMergeItemsSizeBatch           = 2 * 1024 * 1024
	defaultC0MergeMinThreshold     int64 = 2 * 1024 * 1024
	defaultC0MergeMaxThreshold     int64 = 8 * 1024 * 1024
	defaultMergeDirsFlushThreshold       = 4000

	defaultMaxSizeAmplificationRatio = 0.25
	defaultSizeRatio = 0.1
	defaultMinMergeWidth = 2
	defaultMaxMergeWidth = 5
)

// Variables which can be configured to tune how lsm merge work
var (
	MergeItemsSizeBatch     = defaultMergeItemsSizeBatch
	C0MergeMinThreshold     = defaultC0MergeMinThreshold
	C0MergeMaxThreshold     = defaultC0MergeMaxThreshold
	MergeDirsFlushThreshold = defaultMergeDirsFlushThreshold
	MaxSizeAmplificationRatio = defaultMaxSizeAmplificationRatio
    SizeRatio = defaultSizeRatio
    MinMergeWidth = defaultMinMergeWidth
    MaxMergeWidth = defaultMaxMergeWidth
)

type mergeState struct {
	items     *SortedArray.Array //rbtree.OrderedTree
	itemsSize int

	ctrees []*ctree
	// dir entries per each resulting ctree depth level
	// dirs[0] - lowest dirs, dirs[max] - root
	dirs  [][]dirEntry
	stats ctreeStats

	// writer responsible for writing merge result
	writer io.MergeWriter

	// iterator, which iterates over entries to be merged and saved
	it *Iter

	// bloom filter for merged tree
	bloom     *ctreeBloom
	bloomOffs int64
	bloomSize int64
}

// mergeSizeThreshold returns threshold for ctree leaf sum size at which
// ctree is should be merged into next one
func (ctree *ctree) mergeSizeThreshold() int64 {
	idx := ctree.Idx

	c0Limit := C0MergeMaxThreshold

	if idx < memLsmCtrees {
		return c0Limit
	}
	return c0Limit << (2 * uint(idx-memLsmCtrees))
}

func (ctree *ctree) mergeRequired() bool {
	return atomic.LoadInt64(&ctree.Stats.LeafsSize) > ctree.mergeSizeThreshold()
}

// checkSizeAmplification checks if full merge due to expected size amplification is needed
func (lsm* Lsm) checkSizeAmplification() bool {
	lsm.cfg.Log.Infoln("check size amp:")
	lsm.logSizes()
	if len(lsm.Ctree) <= memLsmCtrees {
		return false
	}
	var sum int64 = 0
	for i := memLsmCtrees; i < len(lsm.Ctree) - 1; i++ {
		sum += lsm.Ctree[i].Stats.LeafsSize
	}
	if sum == 0 {
		lsm.cfg.Log.Infoln("Zero sum")
		return false
	}
	if lsm.Ctree[len(lsm.Ctree) - 1].Stats.LeafsSize == 0 {
		lsm.cfg.Log.Infoln("Zero last")
		return true
	}
	ratio := float64(sum) / float64(lsm.Ctree[len(lsm.Ctree) - 1].Stats.LeafsSize)
	if ratio > MaxSizeAmplificationRatio {
		lsm.cfg.Log.Infoln("Amp ratio: ", ratio)
		return true
	}
	lsm.cfg.Log.Infoln("Amp no results")
	return false
}

// checkSizeAmplification checks if full merge due to too big size ratio is needed
func (lsm* Lsm) checkSizeRatio(idx int) (bool, int, int) {
	var bound = idx + 1
	var sum = lsm.Ctree[idx].Stats.LeafsSize
	nonEmptyTrees := 0
	if sum != 0 {
		nonEmptyTrees = 1
	}

	for (nonEmptyTrees != MaxMergeWidth) && (bound < len(lsm.Ctree)) {
		currentSize := lsm.Ctree[bound].Stats.LeafsSize
		if (sum == 0) || (float64(currentSize) / float64(sum) <= (100 + SizeRatio) / 100.0) {
			sum += currentSize
			bound++
			if currentSize != 0 {
				nonEmptyTrees++
			}
		} else {
			break
		}
	}

	bound--

	var mergeRequired = (bound != idx) && (nonEmptyTrees >= MinMergeWidth)
	var next = bound + 1

	if next == len(lsm.Ctree) {
		next = 0
	}

	return mergeRequired, bound, next
}

// mergeParams returns (false, _, next) if ctree on idx should not be merged and (true, bound, next) otherwise,
// where next is the next tree which should be checked and bound is the last tree which should be merged
func (lsm* Lsm) mergeParams(idx int) (bool, int, int) {
	var mergeRequired bool
	var next int
	lsm.cfg.Log.Infoln("checking merge strt idx: ", idx)
	if idx < memLsmCtrees {
		ctree := lsm.Ctree[idx]
		mergeRequired = ctree.Stats.LeafsSize > ctree.mergeSizeThreshold()
		next = idx + 1
		if next == len(lsm.Ctree) - 1 {
			next = 0
		}
		return mergeRequired, idx + 1, next
	} else {
		mergeRequired = false
		var bound int
		if idx == memLsmCtrees {
			mergeRequired = lsm.checkSizeAmplification()
			if mergeRequired {
				return true, len(lsm.Ctree) - 1, 0
			}
		}
		if !mergeRequired {
			mergeRequired, bound, next = lsm.checkSizeRatio(idx)
		}
		return mergeRequired, bound, next
	}
}

func (lsm *Lsm) mergeSaveEntries(state *mergeState) (err error) {
	if err = lsm.Error(); err != nil {
		return err
	}

	ctree := state.ctrees[len(state.ctrees)-1]
	dirs, err := ctree.writeEntries(state.items, &state.stats, state.writer)
	state.items.Clear()
	state.items.Prealloc(128 * 1024)
	state.itemsSize = 0
	if err != nil || len(dirs) == 0 {
		return err
	}
	if len(state.dirs) == 0 {
		state.dirs = make([][]dirEntry, 1, 16)
	}
	state.dirs[0] = append(state.dirs[0], dirs...)
	lsm.debug2("mergeSaveEntries: added dirs: %v", dirs)
	return
}

// save dirs from one depth level, adding their block pointers to high level dirs
func (lsm *Lsm) mergeSaveDirs(state *mergeState, idx int) (err error) {
	if err = lsm.Error(); err != nil {
		return err
	}

	var outdirs []dirEntry
	dirs := state.dirs[idx]
	ctree := state.ctrees[len(state.ctrees)-1]
	lsm.debug2("mergeSaveDirs: saving dirs %v", dirs)
	outdirs, err = ctree.writeDirs(dirs, &state.stats, state.writer)
	if err != nil || len(outdirs) == 0 {
		return
	}
	if idx+1 >= len(state.dirs) {
		state.dirs = append(state.dirs, make([]dirEntry, 0, 1024))
	}
	state.dirs[idx+1] = append(state.dirs[idx+1], outdirs...)
	state.dirs[idx] = nil
	lsm.debug2("mergeSaveDirs: outdirs dirs %v", outdirs)
	return
}

func (lsm *Lsm) mergeSaveDirsIfNeeded(state *mergeState) (err error) {
	for i := 0; i < len(state.dirs); i++ {
		if len(state.dirs[i]) < MergeDirsFlushThreshold {
			continue
		}

		err = lsm.mergeSaveDirs(state, i)
		if err != nil {
			return
		}
	}
	return
}

func (lsm *Lsm) mergeSaveBloomIfNeeded(state *mergeState) (err error) {
	if err = lsm.Error(); err != nil {
		return err
	}

	if state.stats.Count == 0 {
		state.bloom = nil
		return
	}

	bloom := state.bloom
	writer := state.writer
	if bloom == nil {
		return
	}
	if bloom.length() <= 32*1024 { // 32KB; C2 ~hits, C3 ~ not
		// will be written to lsm desc
		return
	}
	lsm.debug2("mergeSaveBloomIfNeeded: saving bloom")

	buf := NewSerializeBuf(bloom.length() + 64)
	bloom.encode(buf, true)
	if state.bloomOffs, err = writer.WritePages(buf.Bytes()); err != nil {
		return err
	}
	assert(state.bloomOffs != 0)
	state.bloomSize = int64(buf.Len())
	lsm.debug2("mergeSaveBloomIfNeeded: saved at %v", state.bloomOffs)
	return
}

func (lsm *Lsm) mergeSaveAll(state *mergeState) (rootOffs int64, rootSize int64, err error) {
	err = lsm.mergeSaveEntries(state)
	if err != nil {
		return
	}

	for i := 0; i < len(state.dirs); i++ {
		// top-level: if single dir left, let it be root. otherwise, save it, and continue until the only entry left
		if i == len(state.dirs)-1 && len(state.dirs[i]) == 1 {
			rootOffs, rootSize = state.dirs[i][0].offs, state.dirs[i][0].size
			break
		}
		err = lsm.mergeSaveDirs(state, i)
		if err != nil {
			return
		}
	}

	if err = lsm.mergeSaveBloomIfNeeded(state); err != nil {
		return
	}

	// C1 can't mutate while merged to disk... if it happens, then someone has modified objects AFTER lsm.Insert()
	root := state.ctrees[0].getRoot()
	assert(state.ctrees[0].Idx != 1 || root.stateCsum == stateCsum(root.items))
	return
}

func adjustFromTo(from, to int) (int, int) {
	if from == 0 {
		from = 1
	}
	if to == 1 {
		to = 2
	}
	assert(from < to)

	return from, to
}

func (lsm *Lsm) pushToUp(to int) int {
	if lsm.Ctree[to].merging {
		return to
	}
	if !lsm.Ctree[to].Empty() {
		assert(to == memLsmCtrees)
		return to
	}
	for (to < len(lsm.Ctree)) && (!lsm.Ctree[to].merging) && (lsm.Ctree[to].Empty()) {
		to++
	}
	return to - 1
}

func (lsm *Lsm) isMergingInRange(from, to int) bool {
	merging := false
	for _, ctree := range lsm.Ctree[from : to+1] {
		merging = merging || ctree.merging
	}
	return merging
}

func (lsm *Lsm) isEmptyInRange(from, to int) bool {
	empty := true
	for _, ctree := range lsm.Ctree[from : to+1] {
		empty = empty && ctree.Empty()
	}
	return empty
}

// called under lsm.Lock()
func (lsm *Lsm) mergeStart(from, to int) {
	lsm.cfg.Log.Infoln("we want: ", from, to)
	from, to = adjustFromTo(from, to)
	if from == 1 {
		to = lsm.pushToUp(to)
	}
	lsm.cfg.Log.Infoln("while we get: ", from, to)

	if to == len(lsm.Ctree) {
		lsm.addCtree()
	}

	if lsm.isMergingInRange(from, to) || lsm.Error() != nil {
		return
	}

	lsm.logSizes()
	if from == 1 {
		assert(lsm.C1.Empty())
		lsm.C1.stealRoot(lsm.C0)
	}

	emptyFrom := lsm.isEmptyInRange(from, to-1)
	emptyTo := lsm.Ctree[to].Empty()
	if emptyFrom {
		return
	}

	if from >= 2 && to == from+1 && emptyTo {
		lsm.debug("merge '%v' %v->%v, just moved", lsm.cfg.Name, from, to)
		lsm.Ctree[to].stealRoot(lsm.Ctree[from])
		return
	}

	for _, ctree := range lsm.Ctree[from : to+1] {
		assert(!ctree.merging)
		ctree.merging = true
	}

	go lsm.mergeGor(from, to)
}

// check if there are non-empty ctrees after the one in question
// it can be easily after annihilation of items that we have higher ctrees present in the array, but they are empty
// e.g. imagine we merge to C3, and C4 is present but empty because everything was removed recently
func (lsm *Lsm) isLastCtree(idx int) bool {
	for idx++; idx < len(lsm.Ctree); idx++ {
		if lsm.Ctree[idx].RootSize > 0 {
			return false
		}
	}
	return true
}

// merge from ctree with given index to next bigger ctree
func (lsm *Lsm) mergeGor(from, to int) {
	lsm.cfg.Log.Infoln(from, to)
	lsm.logSizes()
	err := lsm.doMerge(from, to)
	if err != nil {
		lsm.setError(err)
		lsm.cfg.Log.WithError(err).Errorf("mergeGor: failed")
		lsm.debug("merge '%v': err: %v", lsm.cfg.Name, err)
	}

	lsm.Lock()
	for _, ctree := range lsm.Ctree[from : to+1] {
		ctree.merging = false
	}
	//lsm.popupCtree(to)
	lsm.mergeDone.Broadcast()
	lsm.logSizes()
	lsm.Unlock()
}

func (lsm *Lsm) doMerge(from, to int) (err error) {
	if err = lsm.Error(); err != nil {
		return
	}

	lsm.debug("merge '%v' %v->%v", lsm.cfg.Name, from, to)

	state := lsm.prepareMergeState(from, to)

	rootOffs, rootSize, err := lsm.mergeTrees(state)
	if err != nil {
		_ = state.writer.Abort() // ignore error as we are already in error state
		return
	}

	// finalize merge
	if rootSize == 0 {
		err = state.writer.Abort()
	} else {
		err = state.writer.Close()
	}

	if err != nil {
		return
	}

	srcIds := make([]io.CtreeID, len(state.ctrees))
	for i := range state.ctrees {
		srcIds[i] = state.ctrees[i].getCurrentID()
	}

	// successful merge
	lsm.Lock()
	last := len(state.ctrees) - 1
	for i := 0; i < last; i++ {
		state.ctrees[i].setupRoot(0, 0, 0, &ctreeStats{})
		state.ctrees[i].setupBloom(0, 0, nil)
	}
	state.ctrees[last].setupRoot(rootOffs, rootSize, state.ctrees[last].mergeDiskGen, &state.stats)
	state.ctrees[last].setupBloom(state.bloomOffs, state.bloomSize, state.bloom)
	lsm.debug("merge '%v' done %v->%v (ctree depth %v, size %v, count %v, del %v)",
		lsm.cfg.Name, from, to, len(state.dirs), state.stats.LeafsSize+state.stats.DirsSize, state.stats.Count, state.stats.Deleted)
	lsm.Unlock()

	// freeing blocks of just merged ctrees
	// NOTE: blocks in cache are not invalidated because ctrees are immutable
	// NOTE: so not needed blocks will be preempted: it's just a matter of time
	if err = lsm.cfg.onCtreeCreated(state.ctrees[len(state.ctrees)-1]); err != nil {
		return
	}
	for i := 0; i < len(state.ctrees); i++ {
		ctree := state.ctrees[i]
		if ctree.Idx < memLsmCtrees {
			continue
		}
		if srcIds[i].DiskGen == 0 {
			continue
		}
		nodeBlocks := state.it.citer[i].nodeBlocks
		// add bloom filter range to free it too
		if ctree.BloomOffs != 0 {
			nodeBlocks = append(nodeBlocks, io.CTreeNodeBlock{CtreeID: srcIds[i], Offs: ctree.BloomOffs, Size: ctree.BloomSize})
		}
		if err = lsm.cfg.freeCtreeBlocks(srcIds[i], nodeBlocks); err != nil {
			return
		}
	}
	return
}

// if result of the merge became much smaller due to deleted elements, than pop it up to the upper levels
func (lsm *Lsm) popupCtree(n int) {
	for i := n - 1; i >= memLsmCtrees; i-- {
		if !lsm.Ctree[i].Empty() || atomic.LoadInt64(&lsm.Ctree[i+1].Stats.LeafsSize) > lsm.Ctree[i].mergeSizeThreshold()/2 {
			break
		}
		lsm.Ctree[i].stealRoot(lsm.Ctree[i+1])
	}
}

func (lsm *Lsm) prepareMergeState(from, to int) *mergeState {
	// 1. create mergeState
	lsm.Lock()
	last := lsm.isLastCtree(to)
	assert(!lsm.isEmptyInRange(from, to-1))
	lsm.Ctree[to].mergeDiskGen = lsm.gens.nextCTreeDiskGen(lsm.Ctree[to].Idx)
	state := &mergeState{
		items:  SortedArray.New(lsm.cfg.Comparator, lsm.cfg.ComparatorWithKey).Prealloc(128 * 1024),
		writer: lsm.io.NewMergeWriter(lsm.Ctree[to].getMergeID()),
	}
	state.ctrees = make([]*ctree, to-from+1)
	for i := 0; i < len(state.ctrees); i++ {
		state.ctrees[i] = lsm.Ctree[from+i]
	}
	lsm.Unlock()

	// 2. create iterator over entries to be merged
	state.it = lsm.findEx(GE, nil, nil, from, to)
	if last {
		// do not merge deleted entries in case merging into last ctree
		state.it = state.it.Filter(entryIsNotDeleted)
	}
	state.it.enableBlocksTracking()

	// 3. prepare bloom filter for merged tree
	if lsm.cfg.EnableBloomFilter {
		expectedItems := state.ctrees[0].Stats.Count + state.ctrees[1].Stats.Count
		if last {
			expectedItems -= state.ctrees[0].Stats.Deleted + state.ctrees[1].Stats.Deleted
		}
		state.bloom = newCtreeBloomDefault(int(expectedItems))
	}

	return state
}

// mergeTrees iterates over entries of trees to be merged and actually saves them using merge writer
func (lsm *Lsm) mergeTrees(state *mergeState) (rootOffs, rootSize int64, err error) {
	it := state.it
	from := state.ctrees[0].Idx
	to := state.ctrees[len(state.ctrees)-1].Idx

	count := 0
	for ; !it.Empty() && !lsm.cancel; it.Next() {
		lsm.debug2("merge: %v -> %v, item %v", from, to, it.Result)
		state.items.Insert(it.Result)
		if state.bloom != nil {
			state.bloom.insert(it.Result.GetKey())
		}
		state.itemsSize += it.Result.Size()
		if state.itemsSize > MergeItemsSizeBatch {
			if err = lsm.mergeSaveEntries(state); err != nil {
				return
			}

			if err = lsm.mergeSaveDirsIfNeeded(state); err != nil {
				return
			}
		}
		count++
		if count > 10000 {
			runtime.Gosched()
			count = 0
		}
	}
	if err = it.Error(); err != nil {
		return
	}

	rootOffs, rootSize, err = lsm.mergeSaveAll(state)
	if err != nil {
		return
	}

	return
}

// WaitMergeDone waits until ongoing merge is complete
func (lsm *Lsm) WaitMergeDone() {
	lsm.Lock()
	lsm.waitMergeDoneLocked(0, maxLsmCtrees)
	lsm.Unlock()
}

func (lsm *Lsm) waitMergeDoneLocked(from, to int) {
	from, to = adjustFromTo(from, to)
	for {
		isMerging := false
		curTo := minInt(to, len(lsm.Ctree)-1) // num of ctrees could change while waited
		for _, ctree := range lsm.Ctree[from : curTo+1] {
			if ctree.merging {
				lsm.mergeDone.Wait()
				isMerging = true
			}
		}
		if !isMerging {
			break
		}
	}
}

func (lsm *Lsm) isMergeInProgress() bool {
	for _, ctree := range lsm.Ctree {
		if ctree.merging {
			return true
		}
	}
	return false
}
