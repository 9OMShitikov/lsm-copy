package lsm

import (
	"unsafe"
)

// Filter is predicate for filtering values while iterating
type Filter func(Entry) bool

// Iter is iterator over lsm trees
type Iter struct {
	key    EntryKey
	keyMax EntryKey
	op     int
	Result Entry

	lsm        *Lsm
	generation uint64      // generation of LSM tree (lsm.Generation) when iterator created
	citer      []ctreeIter // array of ctree iterators, citer[0] <-> lsm.Ctree[citerFrom], ...
	ctrees     []*ctree
	eqmask     uint
	err        error

	filters   []Filter // used for filtering resulting nodes
	limit     int      // max number of elements to return
	nextCount int      // number of successful Next() calls == number of returned elements - 1
}

// FindRange returns iterator, which iterates through LSM elements `e` so:
// if op == GT (GE) ==> key < (<=) Key(e) <= toKey
// if op == LT (LE) ==> toKey <= Key(e) < (<=) key
func (lsm *Lsm) FindRange(op int, fromKey, toKey EntryKey) (iter *Iter) {
	return lsm.findEx(op, fromKey, toKey, lsm.Ctree).Filter(entryIsNotDeleted)
}

// Find returns iterator iterating through elements `e` so:
// if op == GT (GE)
func (lsm *Lsm) Find(op int, key EntryKey) (iter *Iter) {
	return lsm.findEx(op, key, nil, lsm.Ctree).Filter(entryIsNotDeleted)
}

// Search just tries to find single entry with given key in Lsm
func (lsm *Lsm) Search(key EntryKey) (entry Entry, err error) {
	iter := lsm.Find(EQ, key)
	return iter.Result, iter.Error()
}

// findEx returns iterator, which iterates through LSM elements
// op
//     used to specify the direction of iterator (less, greater, equals) and to
//     also specify if iterator should include or exclude lower bound key (ex. greater OR greater or equals)
// key, keyMax
//     used to specify returned iterator lower and upper bound (included)
// filters
//     array of functions to filter out entries; filtered entries will not show up at iterators output
// from, to
//     used to specify range of ctrees to search for suitable elements for iteration
func (lsm *Lsm) findEx(op int, key, keyMax EntryKey, ctrees []*ctree) (it *Iter) {
	lsm.RLock()
	defer lsm.RUnlock()

	if op == EQ {
		keyMax = key
	}

	it = &Iter{
		lsm: lsm, op: op,
		key: key, keyMax: keyMax,
		generation: lsm.Generation,
		limit: -1,
	}

	it.ctrees = make([]*ctree, len(ctrees))
	copy(it.ctrees, ctrees)

	it.citer = make([]ctreeIter, len(ctrees))

	entryKeyCmp := lsm.cfg.ComparatorWithKey
	for idx, ct := range ctrees {
		if ct.Empty() {
			continue
		}

		it.citer[idx] = ct.NewIter(op, key, keyMax)
		citer := &it.citer[idx]

		err := citer.Find(true)

		if it.err = err; err != nil {
			break
		}

		if op == EQ && citer.Item() != nil && entryKeyCmp(citer.Item(), key) == 0 {
			// if element found in current ctree we can freely skip lookup in next ctrees
			break
		}
	}
	it.selectOne()
	return
}

// select the minimum element from most recent ctree
func (it *Iter) selectOne() {
	if it.err != nil {
		it.Result = nil
		return
	}

	cmpFunc := it.lsm.cfg.Comparator
	var min interface{}
	opgt := it.op == GE || it.op == GT
	it.eqmask = 0
	assert(len(it.citer) <= int(unsafe.Sizeof(it.eqmask))*8)
	for i := 0; i < len(it.citer); i++ {
		bit := uint(i)
		citem := it.citer[i].Item()
		//debug("select %v", citem)
		if citem == nil {
			continue
		}
		if min == nil {
			min = citem
			it.eqmask = 1 << bit
			continue
		}
		cmp := cmpFunc(min, citem)
		if (opgt && cmp > 0) || (!opgt && cmp < 0) {
			min = citem
			it.eqmask = 1 << bit
		}
		if cmp == 0 {
			it.eqmask |= 1 << bit
		}
	}
	if min == nil {
		it.Result = nil
	} else {
		it.Result = min.(Entry)
	}
}

func entryIsNotDeleted(e Entry) bool {
	return !e.LsmDeleted()
}

// Empty checks iteration complete
func (it *Iter) Empty() bool {
	return it.Result == nil || it.nextCount == it.limit
}

func (it *Iter) Error() error {
	return it.err
}

func (it *Iter) needRestart() bool {
	if it.generation == it.lsm.Generation {
		return false
	}

	// check more fine grained way...
	for idx, ctree := range it.ctrees {
		if it.citer[idx].Generation != ctree.Generation { // ctree was modified
			return true
		}
	}

	// false alarm.
	// can happen in case of merge iterators, which care for changes in 2 ctrees only
	it.generation = it.lsm.Generation
	return false
}

func (it *Iter) restartIfNeeded() {
	if !it.needRestart() {
		return
	}

	key := it.Result.GetKey()

	op := GT
	if it.op == LE || it.op == LT {
		op = LT
	}

	// number of ctrees could have changed, add them to it.citer array then

	newCtrees := it.lsm.Ctree[memLsmCtrees: memLsmCtrees +
											len(it.lsm.Ctree) - len(it.ctrees)]
	for _, ctree := range newCtrees {
		citer := ctree.NewIter(op, key, it.keyMax)
		if it.err = citer.Find(true); it.err != nil {
			return
		}
		it.citer = append(it.citer, citer)
		it.ctrees = append(it.ctrees, ctree)
	}

	// restart those ctree iters which were changed
	for idx, ctree := range it.ctrees {
		citer := &it.citer[idx]
		if citer.Generation == ctree.Generation {
			continue
		}

		*citer = ctree.NewIter(op, key, it.keyMax)
		it.eqmask &^= (1 << uint(idx))
		if it.err = citer.Find(true); it.err != nil {
			break
		}
	}

	it.generation = it.lsm.Generation
}

// Next will try to find next lsm entry accordingly to order operation given at iterator initialization (LT, GT, ...)
// In case given operation is GT, GE or EQ : Next() will search in key-ascending order
// In case given operation is LT or LE     : Next() will search in key-descending order
func (it *Iter) Next() {
	it.lsm.RLock()
	defer it.lsm.RUnlock()
	assert(it.op != EQ)
	assert(!it.Empty())

	it.restartIfNeeded()
	it.nextOne()
	it.skipFiltered()

	it.nextCount++
}

func (it *Iter) nextOne() {
	for i := 0; i < len(it.citer) && it.err == nil; i++ {
		if it.eqmask&(1<<uint(i)) != 0 {
			it.err = it.citer[i].Next()
		}
	}
	it.selectOne()
}

func (it *Iter) enableBlocksTracking() {
	for i := 0; i < len(it.citer); i++ {
		it.citer[i].trackBlocks = true
	}
}

// Filter adds new filter to iterator and returns modified iterator
// After the call iterator will return elements, which satisfy to given
// filter (filter func. returns false on them)
func (it *Iter) Filter(f Filter) *Iter {
	it.lsm.RLock()
	defer it.lsm.RUnlock()

	it.filters = append(it.filters, f)
	if it.Empty() {
		// needed because restartIfNeeded() uses las key to refresh iterators
		// in case iterator is empty it is ok just to keep it Empty on Filter() call
		return it
	}

	it.restartIfNeeded()
	it.skipFiltered()
	return it
}

// Limit sets up limit on number of elements this iterator will return
// before becoming Empty(); Default value is 0 which means no limits;
func (it *Iter) Limit(limit int) *Iter {
	if limit <= 0 {
		limit = -1
	}
	it.limit = limit
	return it
}

func (it *Iter) skipFiltered() {
next:
	for !it.Empty() && it.Error() == nil {
		for _, f := range it.filters {
			if !f(it.Result) {
				if it.op == EQ {
					// if something is filtered on EQ
					// this something must be equal to desired
					// element, so we just returning here and that's it
					it.Result = nil
					return
				}

				it.nextOne()
				continue next
			}
		}
		break
	}
}

// Items returns array containing all lsm entries
func (lsm *Lsm) Items() (items []interface{}, err error) {
	it := lsm.Find(GE, nil)
	for ; !it.Empty(); it.Next() {
		items = append(items, it.Result)
	}
	return items, it.Error()
}
