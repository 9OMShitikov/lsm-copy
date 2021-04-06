package lsm

import (
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/neganovalexey/search/io"
	SortedArray "github.com/neganovalexey/search/sarray"
)

type MergeConfig  struct {
	MergeItemsSizeBatch int
	C0MergeMinThreshold int64
	C0MergeMaxThreshold int64
	MergeDirsFlushThreshold int

	/*
		In this lsm was used universal style compaction algorithm(https://github.com/facebook/rocksdb/wiki/Universal-Compaction)
		Size amplification works correctly if count of entries in lsm is stable
		(incoming rate of deletion should be similar to rate of insertion)
	*/
	EnableSizeAmplificationCheck bool
	MaxSizeAmplificationRatio float64
	SizeRatio float64
	MinMergeWidth int
	MaxMergeWidth int
}

// defaults for merge-configuring values
var defaultMergeCfg = MergeConfig{
	MergeItemsSizeBatch: 2 * 1024 * 1024,
	C0MergeMinThreshold: 2 * 1024 * 1024,
	C0MergeMaxThreshold: 8 * 1024 * 1024,
	MergeDirsFlushThreshold: 4000,

	EnableSizeAmplificationCheck: false,
	MaxSizeAmplificationRatio: 0.25,
	SizeRatio: 0.1,
	MinMergeWidth: 2,
	MaxMergeWidth: 5,
}

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

	c0Limit := ctree.lsm.cfg.MergeCfg.C0MergeMaxThreshold

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
	lsm.debug("checking size amp:")
	lsm.logSizes()
	if len(lsm.Ctree) <= memLsmCtrees {
		return false
	}
	var sum int64 = 0
	for i := memLsmCtrees; i < len(lsm.Ctree) - 1; i++ {
		sum += lsm.Ctree[i].Stats.LeafsSize
	}
	if sum == 0 {
		return false
	}
	if lsm.Ctree[len(lsm.Ctree) - 1].Stats.LeafsSize == 0 {
		return true
	}
	ratio := float64(sum) / float64(lsm.Ctree[len(lsm.Ctree) - 1].Stats.LeafsSize)
	if ratio > lsm.cfg.MergeCfg.MaxSizeAmplificationRatio {
		lsm.debug("Amp ratio: ", ratio)
		return true
	}
	lsm.debug("Amp no results")
	return false
}

// checkSizeRatio checks if full merge due to too big size ratio is needed
func (lsm* Lsm) checkSizeRatio(idx int) (bool, int, int) {
	var bound = idx + 1
	var sum = lsm.Ctree[idx].Stats.LeafsSize
	nonEmptyTrees := 0
	if sum != 0 {
		nonEmptyTrees = 1
	}

	for (nonEmptyTrees != lsm.cfg.MergeCfg.MaxMergeWidth) && (bound < len(lsm.Ctree)) {
		currentSize := lsm.Ctree[bound].Stats.LeafsSize
		if (sum == 0) || (float64(currentSize) / float64(sum) <= (100 + lsm.cfg.MergeCfg.SizeRatio) / 100.0) {
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

	var mergeRequired = (bound != idx) && (nonEmptyTrees >= lsm.cfg.MergeCfg.MinMergeWidth)
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
		if idx == memLsmCtrees && lsm.cfg.MergeCfg.EnableSizeAmplificationCheck {
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
		if len(state.dirs[i]) < lsm.cfg.MergeCfg.MergeDirsFlushThreshold {
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

func (lsm *Lsm) expandIfNeeded() bool {
	lsm.pushCtreesUp()
	if !lsm.Ctree[memLsmCtrees].Empty() {
		lsm.addCtree()
		buf := lsm.Ctree[len(lsm.Ctree) - 1]
		copy(lsm.Ctree[memLsmCtrees + 1: len(lsm.Ctree)], lsm.Ctree[memLsmCtrees: len(lsm.Ctree) - 1])
		lsm.Ctree[memLsmCtrees] = buf
		return true
	}
	return false
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
	from, to = adjustFromTo(from, to)

	if lsm.isMergingInRange(from, to) || lsm.Error() != nil {
		return
	}

	if from == memLsmCtrees - 1 {
		lsm.expandIfNeeded()
		to = lsm.pushToUp(to)
	}
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

	lsm.debug("merging:")
	lsm.logSizes()
	ctrees := make([]*ctree, to + 1 - from)
	copy(ctrees, lsm.Ctree[from : to+1])
	go lsm.mergeGor(ctrees)
}

// check if it is the last tree
// (due to compaction algorithm if there is sequence of empty trees in the end of ctrees list
// results of merge will be put to last tree in the list)
func (lsm *Lsm) isLastCtree(ctree *ctree) bool {
	if ctree.Idx == memLsmCtrees {
		return true
	}
	return false
}

// merge from ctree with given index to next bigger ctree
func (lsm *Lsm) mergeGor(ctrees []*ctree) {
	logs := "merge indices: "
	for _, ctree := range ctrees {
		logs += strconv.Itoa(ctree.Idx) + ", "
	}
	lsm.debug(logs)
	err := lsm.doMerge(ctrees)
	if err != nil {
		lsm.setError(err)
		lsm.cfg.Log.WithError(err).Errorf("mergeGor: failed")
		lsm.debug("merge '%v': err: %v", lsm.cfg.Name, err)
	}

	lsm.Lock()
	for _, ctree := range ctrees {
		ctree.merging = false
	}
	lsm.mergeDone.Broadcast()
	lsm.debug("merged, after merge sizes: ")
	lsm.logSizes()
	lsm.Unlock()
}

func (lsm *Lsm) doMerge(ctrees []*ctree) (err error) {
	if err = lsm.Error(); err != nil {
		return
	}

	state := lsm.prepareMergeState(ctrees)

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

// pushing ctrees up if there is free space
func (lsm *Lsm) pushCtreesUp() {
	start := memLsmCtrees
	finish := start
	for finish < len(lsm.Ctree) {
		for finish < len(lsm.Ctree) && !lsm.Ctree[finish].merging {
			finish++
		}

		if finish != start {
			nonEmptyCtrees := 0
			for i := finish - 1; i >= start; i-- {
				if !lsm.Ctree[i].Empty() {
					nonEmptyCtrees++
					if i != finish - nonEmptyCtrees {
						lsm.Ctree[finish - nonEmptyCtrees].stealRoot(lsm.Ctree[i])
					}
				}
			}
		}

		finish++
		start = finish
	}
}

func (lsm *Lsm) prepareMergeState(ctrees []*ctree) *mergeState {
	// 1. create mergeState
	lsm.Lock()
	to := ctrees[len(ctrees) - 1]
	last := lsm.isLastCtree(ctrees[len(ctrees) - 1])
	to.mergeDiskGen = lsm.gens.nextCTreeDiskGen(to.Idx)
	state := &mergeState{
		items:  SortedArray.New(lsm.cfg.Comparator, lsm.cfg.ComparatorWithKey).Prealloc(128 * 1024),
		writer: lsm.io.NewMergeWriter(to.getMergeID()),
	}
	state.ctrees = ctrees
	lsm.Unlock()

	// 2. create iterator over entries to be merged
	state.it = lsm.findEx(GE, nil, nil, ctrees)
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
		if state.itemsSize > lsm.cfg.MergeCfg.MergeItemsSizeBatch {
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
