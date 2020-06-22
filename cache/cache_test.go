package cache

import (
	"math/rand"
	"testing"

	"github.com/neganovalexey/search/dlist"
	"github.com/stretchr/testify/require"
)

type testValue struct {
	i int
}

func (tv *testValue) OnCacheDropped() {
	// debug("drop: %d", tv.i)
}

func TestCacheWriteRead(t *testing.T) {
	cache := New(2)
	tk := int64(0)
	tag := ""

	cache.Write(tag, tk, &testValue{42}, 1)
	v, _ := cache.Read(tag, tk, func() (CachedObj, int64, error) {
		panic("Unnecessary IO")
	})

	ev := &testValue{42}
	if *(v.(*testValue)) != *ev {
		t.Error("Incorrect values stored or not stored at all")
	}
}

func TestReadInvokedIfMiss(t *testing.T) {
	cache := New(10)
	flag := false
	tag := ""

	_, err := cache.Read(tag, 10, func() (CachedObj, int64, error) {
		flag = true
		return &testValue{1}, 1, nil
	})
	if err != nil {
		t.Error(err)
	}

	if !flag {
		t.Error("Real read not called")
	}
}

func TestSize(t *testing.T) {
	cap := 1000
	cache := New(int64(cap))
	tag := ""

	n := cap / 2

	for i := 0; i < n; i++ {
		cache.Write(tag, int64(i), &testValue{i}, 1)
	}

	if cache.Size() != int64(n) || cache.elementsCnt() != n {
		t.Error("Bad cache size or stored slots count")
	}

	for i := n; i < n+cap; i++ {
		cache.Write(tag, int64(i), &testValue{i}, 1)
	}

	if cache.Size() > cache.capacity {
		t.Error("Cache capacity exceeded: ", cache.Size(), " > ", cache.capacity)
	}

	if cache.elementsCnt() > cap {
		t.Error("Slots count exceeded: ", cache.elementsCnt(), " > ", cap)
	}
}

func TestRandom(t *testing.T) {
	cap := 1000
	cache := New(int64(cap))
	tag := ""

	n := 13 * cap
	values := make(map[int]*testValue)

	for i := 0; i < n; i++ {
		r := rand.Intn(cap * 2)
		v := &testValue{i}
		cache.Write(tag, int64(r), v, 1)
		values[r] = v
	}

	for k, v := range values {
		actual, _ := cache.Read(tag, int64(k), func() (CachedObj, int64, error) {
			return nil, 0, nil
		})
		if actual != nil {
			if *v != *actual.(*testValue) {
				t.Error("Bad value stored at key: ", k)
			}
		}
	}

	for i := 0; i < n; i++ {
		r := rand.Intn(cap * 2)
		v := &testValue{i}
		_, err := cache.Read(tag, int64(r), func() (CachedObj, int64, error) {
			values[r] = v
			return v, 1, nil
		})
		if err != nil {
			t.Error(err)
		}
	}

	for k, v := range values {
		actual, _ := cache.Read(tag, int64(k), func() (CachedObj, int64, error) {
			return nil, 0, nil
		})
		if actual != nil {
			if *v != *actual.(*testValue) {
				t.Error("Bad value stored at key: ", k)
			}
		}
	}

	listLen := func(l *dlist.ListHead) (cnt int) {
		l.ForEach(func(_ *dlist.List) bool {
			cnt++
			return true
		})
		return
	}
	if cache.elementsCnt() > cap {
		t.Error("Not expected cache elements cnt = ", cache.elementsCnt())
	}
	if listLen(cache.activeSlots) > cap/2 {
		t.Error("Not expected active slots = ", listLen(cache.activeSlots))
	}
	if cache.activeSize > int64(cap/2) {
		t.Error("Not expected active size = ", cache.activeSize)
	}
	if listLen(cache.inactiveSlots) > cap/2 {
		t.Error("Not expected inactive slots = ", listLen(cache.inactiveSlots))
	}
	if cache.inactiveSize > int64(cap/2) {
		t.Error("Not expected inactive size = ", cache.inactiveSize)
	}

	t.Log("Average Hit Ratio: ", cache.HitRatio())
}

func TestZeroLimitCache(t *testing.T) {
	cache := New(0)
	tag := "tag"

	for i := 0; i < 1000; i++ {
		i := i
		op := rand.Intn(2)
		switch op {
		case 0:
			cache.Write(tag, int64(rand.Int()%100), &testValue{42}, 1)
		case 1:
			_, err := cache.Read(tag, int64(rand.Intn(100)), func() (CachedObj, int64, error) {
				return &testValue{i}, 1, nil
			})
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func TestWriteDoNotTrashoutCache(t *testing.T) {
	cap := 1000
	cache := New(int64(cap))
	tag := ""

	for i := 0; i < cap/10; i++ {
		_, err := cache.Read(tag, int64(i), func() (CachedObj, int64, error) {
			return &testValue{42}, 1, nil
		})
		require.NoError(t, err)
	}

	for i := cap / 10; i < 2*cap; i++ {
		cache.Write(tag, int64(i), &testValue{43}, 1)
	}

	for i := 0; i < cap/10; i++ {
		_, err := cache.Read(tag, int64(i), func() (CachedObj, int64, error) {
			t.Error("Active slot replaced on massive writes")
			return nil, 1, nil
		})
		require.NoError(t, err)
	}
}

func TestSetCapacity(t *testing.T) {
	cap1 := 1000
	cache := New(int64(cap1))

	for i := 0; i < cap1; i++ {
		i := i
		_, err := cache.Read("", int64(i), func() (CachedObj, int64, error) {
			return &testValue{i}, 1, nil
		})
		require.NoError(t, err)
	}

	if cache.Size() != int64(cap1) {
		t.Fatal("Bad size (not all capacity used)")
	}

	cache.SetCapacity(3)

	if cache.Size() != 3 {
		t.Fatalf("Bad size, expected %v, got: %v", 3, cache.Size())
	}
}

func TestRewriteAdjustsSize(t *testing.T) {
	cache := New(20)

	cache.Write("", 100, &testValue{}, 5)
	require.EqualValues(t, 5, cache.Size())

	cache.Write("", 100, &testValue{}, 7)
	require.EqualValues(t, 7, cache.Size(), "cache size should be adjusted on rewrite if size changes")

	cache.Write("", 100, &testValue{}, 20)
	require.EqualValues(t, 20, cache.Size())

	// elements with size > max should be evicted immediately
	cache.Write("", 100, &testValue{}, 21)
	require.EqualValues(t, 0, cache.Size())
}
