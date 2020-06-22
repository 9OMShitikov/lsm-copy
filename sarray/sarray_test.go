package sarray

import (
	"fmt"
	"testing"

	"github.com/neganovalexey/search/rbtree"
)

func verify(t *testing.T, cond bool, msg string, x ...interface{}) {
	if !cond {
		t.Errorf(msg, x...)
	}
}

func TestIteratorNext(t *testing.T) {
	tree := New(rbtree.IntComparator, rbtree.IntComparator)
	tree.Insert(2)
	tree.Insert(3)
	tree.Insert(7)
	tree.Insert(9)
	tree.Insert(12)
	tree.Insert(20)

	exp := []int{2, 3, 7, 9, 12, 20}
	count := 0
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		verify(t, count < len(exp), "iterating beyond range")
		verify(t, iter.Item() == exp[count], "iterate wrong value %v != %v", iter.Item(), exp[count])
		count++
	}
}

func TestIteratorPrev(t *testing.T) {
	tree := New(rbtree.IntComparator, rbtree.IntComparator)
	tree.Insert(2)
	tree.Insert(3)
	tree.Insert(7)
	tree.Insert(9)
	tree.Insert(12)
	tree.Insert(20)

	exp := []int{20, 12, 9, 7, 3, 2}
	count := 0
	for iter := tree.Last(); !iter.Empty(); iter.Prev() {
		verify(t, count < len(exp), "iterating beyond range")
		verify(t, iter.Item() == exp[count], "iterate wrong value %v != %v", iter.Item(), exp[count])
		count++
	}
}

func TestFind(t *testing.T) {
	tree := New(rbtree.IntComparator, rbtree.IntComparator)
	tree.Insert(2)
	tree.Insert(3)
	tree.Insert(7)
	tree.Insert(9)
	tree.Insert(20)

	verify(t, tree.Count() == 5, "tree size mismatch")
	if actualValue, expectedValue := fmt.Sprintf("%d%d%d%d%d", tree.Items()...), "237920"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	tests1 := [][]interface{}{
		// val, Eq, Ge, Gt, Le, Lt
		{-1, nil, 2, 2, nil, nil},
		{1, nil, 2, 2, nil, nil},
		{2, 2, 2, 3, 2, nil},
		{3, 3, 3, 7, 3, 2},
		{5, nil, 7, 7, 3, 3},
		{7, 7, 7, 9, 7, 3},
		{8, nil, 9, 9, 7, 7},
		{9, 9, 9, 20, 9, 7},
		{11, nil, 20, 20, 9, 9},
		{20, 20, 20, nil, 20, 9},
		{100, nil, nil, nil, 20, 20},
	}

	for _, test := range tests1 {
		iter := tree.FindEq(test[0])
		verify(t, iter.Item() == test[1], "%v != %v", iter.Item(), test[1])

		iter = tree.FindGe(test[0])
		verify(t, iter.Item() == test[2], "%v != %v", iter.Item(), test[2])

		iter = tree.FindGt(test[0])
		verify(t, iter.Item() == test[3], "%v != %v", iter.Item(), test[3])

		iter = tree.FindLe(test[0])
		verify(t, iter.Item() == test[4], "%v != %v", iter.Item(), test[4])

		iter = tree.FindLt(test[0])
		verify(t, iter.Item() == test[5], "%v != %v", iter.Item(), test[5])
	}
}

func benchmarkSearch(b *testing.B, arr *Array, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			arr.Search(n)
		}
	}
}

func benchmarkInsert(b *testing.B, arr *Array, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			arr.Insert(n)
		}
	}
}

func BenchmarkSearch1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	for n := 0; n < size; n++ {
		arr.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, arr, size)
}

func BenchmarkSearch10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	for n := 0; n < size; n++ {
		arr.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, arr, size)
}

func BenchmarkSearch100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	for n := 0; n < size; n++ {
		arr.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, arr, size)
}

func BenchmarkInsert1000(b *testing.B) {
	size := 1000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	benchmarkInsert(b, arr, size)
}

func BenchmarkInsert10000(b *testing.B) {
	size := 10000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	benchmarkInsert(b, arr, size)
}

func BenchmarkInsert100000(b *testing.B) {
	size := 100000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	benchmarkInsert(b, arr, size)
}

func BenchmarkInsert1000000(b *testing.B) {
	size := 1000000
	arr := New(rbtree.IntComparator, rbtree.IntComparator)
	benchmarkInsert(b, arr, size)
}
