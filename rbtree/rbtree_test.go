package rbtree

import (
	"fmt"
	"strconv"
	"testing"
)

func TestRbtreeInsert(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(5)
	tree.Insert(6)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(4)
	tree.Insert(1)
	tree.Insert(2)
	tree.Insert(1) //collision!

	if actualValue := tree.Count(); actualValue != 8 {
		t.Errorf("Got %v expected %v", actualValue, 8)
	}
	t.Log(tree)
	if actualValue, expectedValue := fmt.Sprintf("%d%d%d%d%d%d%d%d", tree.Items()...), "11234567"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}

	tests1 := [][]interface{}{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, 5},
		{6, 6},
		{7, 7},
		{8, nil},
		{-10, nil},
	}

	for _, test := range tests1 {
		item := tree.Search(test[0])
		if item != test[1] {
			t.Errorf("Got %v expected %v", item, test[1])
		}
	}
}

func TestRbtreeRemove(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(5)
	tree.Insert(6)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(4)
	tree.Insert(1)
	tree.Insert(2)
	tree.Insert(1)

	tree.Remove(5)
	tree.Remove(6)
	tree.Remove(7)
	tree.Remove(8)
	tree.Remove(5)

	if actualValue, expectedValue := fmt.Sprintf("%d%d%d%d%d", tree.Items()...), "11234"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if actualValue := tree.Count(); actualValue != 5 {
		t.Errorf("Got %v expected %v", actualValue, 5)
	}

	tests2 := [][]interface{}{
		{1, 1},
		{2, 2},
		{3, 3},
		{4, 4},
		{5, nil},
		{6, nil},
		{7, nil},
		{8, nil},
	}

	for _, test := range tests2 {
		item := tree.Search(test[0])
		if item != test[1] {
			t.Errorf("Got %v expected %v", item, test[1])
		}
	}

	tree.Remove(1)
	tree.Remove(4)
	tree.Remove(2)
	tree.Remove(3)
	tree.Remove(2)
	tree.Remove(2)
	tree.Remove(1)

	if actualValue, expectedValue := fmt.Sprintf("%s", tree.Items()), "[]"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if empty, size := tree.Empty(), tree.Count(); empty != true || size != -0 {
		t.Errorf("Got %v expected %v", empty, true)
	}

}

func TestRbtreeLeftAndRight(t *testing.T) {
	tree := NewWithIntComparator()

	if actualValue := tree.First(); !actualValue.Empty() {
		t.Errorf("Got %v expected %v", actualValue, nil)
	}
	if actualValue := tree.Last(); !actualValue.Empty() {
		t.Errorf("Got %v expected %v", actualValue, nil)
	}

	tree.Insert(1)
	tree.Insert(5)
	tree.Insert(6)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(4)
	tree.Insert(1) // overwrite
	tree.Insert(2)

	if actualValue, expectedValue := fmt.Sprintf("%d", tree.First().Item()), "1"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}

	if actualValue, expectedValue := fmt.Sprintf("%d", tree.Last().Item()), "7"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator1Next(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(5)
	tree.Insert(6)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(4)
	tree.Insert(1)
	tree.Insert(2)
	// │   ┌── 7
	// └── 6
	//     │   ┌── 5
	//     └── 4
	//         │   ┌── 3
	//         └── 2
	//             └── 1
	count := 0
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		count++
		key := iter.Item()
		switch key {
		case count:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
	}
	if actualValue, expectedValue := count, tree.Count(); actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator1Prev(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(5)
	tree.Insert(6)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(4)
	tree.Insert(1)
	tree.Insert(2)
	// │   ┌── 7
	// └── 6
	//     │   ┌── 5
	//     └── 4
	//         │   ┌── 3
	//         └── 2
	//             └── 1
	countDown := tree.Count()
	for iter := tree.Last(); !iter.Empty(); iter.Prev() {
		key := iter.Item()
		switch key {
		case countDown:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
		countDown--
	}
	if actualValue, expectedValue := countDown, 0; actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator2Next(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(3)
	tree.Insert(1)
	tree.Insert(2)
	count := 0
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		count++
		key := iter.Item()
		switch key {
		case count:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
	}
	if actualValue, expectedValue := count, tree.Count(); actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator2Prev(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(3)
	tree.Insert(1)
	tree.Insert(2)
	countDown := tree.Count()
	for iter := tree.Last(); !iter.Empty(); iter.Prev() {
		key := iter.Item()
		switch key {
		case countDown:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
		countDown--
	}
	if actualValue, expectedValue := countDown, 0; actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator3Next(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(1)
	count := 0
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		count++
		key := iter.Item()
		switch key {
		case count:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, count; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
	}
	if actualValue, expectedValue := count, tree.Count(); actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeIterator3Prev(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(1)
	countDown := tree.Count()
	for iter := tree.Last(); !iter.Empty(); iter.Prev() {
		key := iter.Item()
		switch key {
		case countDown:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		default:
			if actualValue, expectedValue := key, countDown; actualValue != expectedValue {
				t.Errorf("Got %v expected %v", actualValue, expectedValue)
			}
		}
		countDown--
	}
	if actualValue, expectedValue := countDown, 0; actualValue != expectedValue {
		t.Errorf("Count different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRbtreeFind(t *testing.T) {
	tree := NewWithIntComparator()
	tree.Insert(31)
	tree.Insert(20)
	tree.Insert(7)
	tree.Insert(3)
	tree.Insert(30)
	tree.Insert(1)
	tree.Insert(2)
	tree.Insert(10)

	if actualValue, expectedValue := fmt.Sprintf("%d%d%d%d%d%d%d%d", tree.Items()...), "123710203031"; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}

	if tree.FindEq(20).Item() != 20 {
		t.Errorf("Got no 20")
	}
	tests1 := [][]interface{}{
		// val, Ge, Gt, Le, Lt
		{-1, 1, 1, nil, nil},
		{1, 1, 2, 1, nil},
		{2, 2, 3, 2, 1},
		{3, 3, 7, 3, 2},
		{5, 7, 7, 3, 3},
		{7, 7, 10, 7, 3},
		{10, 10, 20, 10, 7},
		{11, 20, 20, 10, 10},
		{30, 30, 31, 30, 20},
		{31, 31, nil, 31, 30},
		{40, nil, nil, 31, 31},
	}

	for _, test := range tests1 {
		iter := tree.FindGe(test[0])
		if (iter.Empty() && test[1] != nil) || (!iter.Empty() && iter.Item() != test[1]) {
			t.Errorf("Got %v expected %v", iter, test[1])
		}

		iter = tree.FindGt(test[0])
		if (iter.Empty() && test[2] != nil) || (!iter.Empty() && iter.Item() != test[2]) {
			t.Errorf("Got %v expected %v", iter, test[2])
		}

		iter = tree.FindLe(test[0])
		if (iter.Empty() && test[3] != nil) || (!iter.Empty() && iter.Item() != test[3]) {
			t.Errorf("Got %v expected %v", iter, test[3])
		}

		iter = tree.FindLt(test[0])
		if (iter.Empty() && test[4] != nil) || (!iter.Empty() && iter.Item() != test[4]) {
			t.Errorf("Got %v expected %v", iter, test[4])
		}
	}
}

func benchmarkSearch(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Search(n)
		}
	}
}

func benchmarkInsert(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Insert(n)
		}
	}
}

func benchmarkRemove(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Remove(n)
		}
	}
}

func BenchmarkRbtreeSearch100(b *testing.B) {
	b.StopTimer()
	size := 100
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, tree, size)
}

func BenchmarkRbtreeSearch1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, tree, size)
}

func BenchmarkRbtreeSearch10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, tree, size)
}

func BenchmarkRbtreeSearch100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkSearch(b, tree, size)
}

type Data struct {
	str string
	n   int
	k   uint
}

func DataCmp(a, b interface{}) int {
	s1 := a.(*Data).str
	s2 := b.(*Data).str

	min := len(s2)
	if len(s1) < len(s2) {
		min = len(s1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(s1[i]) - int(s2[i])
	}
	if diff == 0 {
		diff = len(s1) - len(s2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

func DataKeyCmp(a, b interface{}) int {
	s1 := a.(*Data).str
	s2 := b.(string)

	min := len(s2)
	if len(s1) < len(s2) {
		min = len(s1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(s1[i]) - int(s2[i])
	}
	if diff == 0 {
		diff = len(s1) - len(s2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

func BenchmarkRbtreeInsertStr100000(b *testing.B) {
	size := 100000
	for i := 0; i < b.N; i++ {
		tree := New(DataCmp, DataKeyCmp)
		for n := 0; n < size; n++ {
			d := &Data{n: n, k: uint(n) * 2, str: "xxx"}
			d.str = strconv.Itoa(n)
			tree.Insert(d)
		}
	}
}

func BenchmarkRbtreeInsert100(b *testing.B) {
	size := 100
	tree := NewWithIntComparator()
	benchmarkInsert(b, tree, size)
}

func BenchmarkRbtreeInsert1000(b *testing.B) {
	size := 1000
	tree := NewWithIntComparator()
	benchmarkInsert(b, tree, size)
}

func BenchmarkRbtreeInsert10000(b *testing.B) {
	size := 10000
	tree := NewWithIntComparator()
	benchmarkInsert(b, tree, size)
}

func BenchmarkRbtreeInsert100000(b *testing.B) {
	size := 100000
	tree := NewWithIntComparator()
	benchmarkInsert(b, tree, size)
}

func BenchmarkRbtreeInsert1000000(b *testing.B) {
	size := 1000000
	tree := NewWithIntComparator()
	benchmarkInsert(b, tree, size)
}

func BenchmarkRbtreeRemove100(b *testing.B) {
	size := 100
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRbtreeRemove1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRbtreeRemove10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRbtreeRemove100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	tree := NewWithIntComparator()
	for n := 0; n < size; n++ {
		tree.Insert(n)
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}
