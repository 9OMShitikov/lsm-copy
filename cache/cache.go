package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/neganovalexey/search/dlist"
)

// CachedObj is an interface for stored in cache object,
// which has one method to be invoked when object is dropped
// from cache
type CachedObj interface {
	OnCacheDropped()
}

// Key makes it possible to store values with equal offsets in cache
// so it makes cache keys distingushable
type Key interface{}

// NoneCacheID should be used in case there is no need to select sub-cache in lsm node cache
// (it is used to reuse lsm node cache for non-lsm purposes)
var NoneCacheID Key

// IoReadFunc represents callback, which is called in case requested value
// not found in the cache. It should preform all time-consuming IO operations
// to retrieve value to be cached
type IoReadFunc func() (obj CachedObj, size int64, err error)

// BasicCache is a core cache interface which should be enough for basic usage
type BasicCache interface {
	Read(id Key, offs int64, ioRead IoReadFunc) (CachedObj, error)
	Write(id Key, offs int64, obj CachedObj, size int64)
	SetCapacity(newCapacity int64)
	GetCapacity() int64
}

type cacheKey struct {
	id   Key
	offs int64
}

type cacheSlot struct {
	listNode dlist.List
	isActive bool

	key  cacheKey
	size int64
	obj  CachedObj
}

func slotFromList(el *dlist.List) *cacheSlot {
	return (*cacheSlot)(unsafe.Pointer(el))
}

type cacheStats struct {
	reads int64
	hits  int64
}

// Cache is a fixed-capacity container, which uses LRU policy
type Cache struct {
	sync.RWMutex

	capacity    int64
	inactiveCap int64
	activeCap   int64

	// map from key = {id, offset} to corresponding cache slot list element
	cache      map[cacheKey]*cacheSlot
	inProgress map[cacheKey]chan struct{}
	// list for hot nodes
	activeSlots *dlist.ListHead
	activeSize  int64
	// list for not so hot nodes :)
	inactiveSlots *dlist.ListHead
	inactiveSize  int64

	stats cacheStats
}

// New makes new Cache container with given capacity
func New(capacity int64) *Cache {
	return new(Cache).Init(capacity)
}

// Init initializes Cache container for given capacity value
func (cache *Cache) Init(capacity int64) *Cache {
	cache.Lock()
	defer cache.Unlock()

	if capacity < 0 {
		capacity = 0
	}

	cache.capacity = capacity
	cache.activeCap = capacity / 2
	cache.inactiveCap = capacity - cache.activeCap
	cache.cache = make(map[cacheKey]*cacheSlot)
	cache.inProgress = make(map[cacheKey]chan struct{})
	cache.activeSlots = new(dlist.ListHead)

	cache.activeSlots.Init(nil)
	cache.inactiveSlots = new(dlist.ListHead)
	cache.inactiveSlots.Init(nil)

	return cache
}

// Size returns current cache load
func (cache *Cache) Size() int64 {
	return cache.activeSize + cache.inactiveSize
}

// SetCapacity changes current cache capacity to given value and removes LRU slots
// so current used size match that new bound
func (cache *Cache) SetCapacity(newCapacity int64) {
	cache.Lock()
	defer cache.Unlock()
	assert(newCapacity >= 0)
	cache.capacity = newCapacity
	cache.activeCap = newCapacity / 2
	cache.inactiveCap = newCapacity - cache.activeCap
	cache.rebalance()
}

// GetCapacity returns current cache capacity
func (cache *Cache) GetCapacity() (res int64) {
	cache.RLock()
	res = cache.capacity
	cache.RUnlock()
	return
}

func (cache *Cache) rebalance() {
	for cache.activeSize > cache.activeCap {
		lru := cache.activeSlots.Last()
		slot := slotFromList(lru)

		// active list lru becomes mru in inactive list
		assert(slot.isActive)
		slot.isActive = false
		cache.activeSize -= slot.size
		cache.inactiveSize += slot.size
		cache.inactiveSlots.Move(&slot.listNode)
	}

	// NOTE: we don't really care about inactive list size. we care about *total* limit.
	// if we check inactive size here, than for large objects effective cache size may become equal to cache.inactiveCap
	// e.g. cache.capacity = 4MB, object = 4MB. it will be moved to inactive and than destroyed immediately as it's > cache.inactiveCap.
	for cache.activeSize+cache.inactiveSize > cache.capacity {
		lru := cache.inactiveSlots.Last()
		slot := slotFromList(lru)
		cache.removeSlot(slot)
	}
}

func (cache *Cache) addToList(slot *cacheSlot, list *dlist.ListHead, size *int64) {
	*size += slot.size
	list.Move(&(slot.listNode))

	cache.cache[slot.key] = slot

	cache.rebalance()
}

func (cache *Cache) getSlot(id Key, offs int64) (slot *cacheSlot, isIn bool) {
	slot, isIn = cache.cache[cacheKey{id, offs}]
	return
}

func (cache *Cache) addActive(slot *cacheSlot) {
	slot.isActive = true
	cache.addToList(slot, cache.activeSlots, &cache.activeSize)
}

func (cache *Cache) addInactive(slot *cacheSlot) {
	slot.isActive = false
	cache.addToList(slot, cache.inactiveSlots, &cache.inactiveSize)
}

// moveActive moves slot from inactive list to active and rebalances lists
func (cache *Cache) moveActive(slot *cacheSlot) {
	assert(!slot.isActive)
	cache.inactiveSize -= slot.size
	cache.addActive(slot)
}

func (cache *Cache) removeSlot(slot *cacheSlot) {
	slot.listNode.Remove()

	slot, isIn := cache.cache[slot.key]
	assert(isIn)
	delete(cache.cache, slot.key)

	if slot.isActive {
		cache.activeSize -= slot.size
	} else {
		cache.inactiveSize -= slot.size
	}
	slot.obj.OnCacheDropped()
	slot.obj = nil
}

// Write simply adds new node to cache and maybe drops some nodes
// from cache in case there is no enough space for a new one
func (cache *Cache) Write(id Key, offs int64, obj CachedObj, size int64) {
	cache.Lock()
	defer cache.Unlock()

	slot, isIn := cache.getSlot(id, offs)

	if isIn {
		cache.rewriteSlot(slot, obj, size)
		return
	}

	slot = &cacheSlot{key: cacheKey{id, offs}, obj: obj, size: size}
	cache.addInactive(slot)
}

// rewrites object of already existing slot, adjusts slot list size,
// marks slot as MRU and rebalances cache
func (cache *Cache) rewriteSlot(slot *cacheSlot, newObj CachedObj, newSize int64) {
	oldSize := slot.size
	slot.obj = newObj
	slot.size = newSize

	if slot.isActive {
		cache.activeSize += newSize - oldSize
		cache.activeSlots.Move(&slot.listNode)
	} else {
		cache.inactiveSize += newSize - oldSize
		cache.inactiveSlots.Move(&slot.listNode)
	}

	cache.rebalance()
}

func (cache *Cache) hit(slot *cacheSlot) {
	// should be called under lock

	cache.stats.hits++
	if slot.isActive {
		// make slot MRU in active list
		cache.activeSlots.Move(&slot.listNode)
	} else {
		// make it active if it is not yet
		cache.moveActive(slot)
	}
}

// Read searches for cached node and in case of failure calls ioRead callback, which
// intended to perform real IO or something to read a node
func (cache *Cache) Read(id Key, offs int64, ioRead IoReadFunc) (obj CachedObj, err error) {
	atomic.AddInt64(&cache.stats.reads, 1)
	key := cacheKey{id, offs}
	var size int64
	var waitCh chan struct{}
	var ok bool

start:
	cache.Lock()
	slot, isIn := cache.getSlot(id, offs)
	if isIn {
		cache.hit(slot)
		obj := slot.obj
		cache.Unlock()
		return obj, nil
	}

	waitCh, ok = cache.inProgress[key]
	if ok {
		cache.Unlock()
		<-waitCh
		goto start
	}

	waitCh = make(chan struct{})
	cache.inProgress[key] = waitCh
	cache.Unlock()

	obj, size, err = ioRead()

	cache.Lock()
	delete(cache.inProgress, key)
	defer func() {
		cache.Unlock()
		close(waitCh)
	}()

	if obj == nil || err != nil {
		return nil, err
	}

	cache.addActive(&cacheSlot{key: key, obj: obj, size: size})
	return
}

// HitRatio returns cache hit ratio = hits / reads
func (cache *Cache) HitRatio() float32 {
	cache.RLock()
	defer cache.RUnlock()
	if cache.stats.reads == 0 {
		return 0
	}
	return float32(cache.stats.hits) / float32(cache.stats.reads)
}

// elementsCnt returns number of stored elements in cache
func (cache *Cache) elementsCnt() (n int) {
	return len(cache.cache)
}

func assert(cond bool) {
	if cond {
		return
	}
	panic("Fatal assertion error")
}
