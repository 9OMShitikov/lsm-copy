package sarray

import (
	"fmt"

	"github.com/neganovalexey/search/rbtree"
)

// Array (sorted array) is a container for already sorted entries:
// - entries can be added to the end of the array (but no verification of ordering)
// - entries can be searched for using built-in binary search
//
// It implements same interfaces as rbtree and its iterator do
type Array struct {
	nodes             []Node
	count             int
	Comparator        rbtree.Comparator
	ComparatorWithKey rbtree.Comparator
}

// Node describes single array node
type Node interface{}

// nolint: unused, deadcode
func assertInterfaces() {
	var _ rbtree.OrderedTree = (*Array)(nil)
	var _ rbtree.OrderedIter = (*Iter)(nil)
}

// New creates empty array
func New(comparator rbtree.Comparator, comparatorWithKey rbtree.Comparator) *Array {
	return &Array{Comparator: comparator, ComparatorWithKey: comparatorWithKey}
}

// Prealloc preallocates memory for n nodes
func (arr *Array) Prealloc(count int) *Array {
	if arr.count != 0 {
		panic("prealloc: not empty")
	}
	arr.nodes = make([]Node, 0, count)
	return arr
}

// Insert item to array
func (arr *Array) Insert(item interface{}) (existing interface{}) {
	arr.nodes = append(arr.nodes, item)
	arr.count++
	return nil
}

// Search item by key
func (arr *Array) Search(key interface{}) (item interface{}) {
	idx := arr.lookup(key)
	if idx < 0 {
		return nil
	}
	return arr.nodes[idx]
}

// Remove by key
func (arr *Array) Remove(key interface{}) {
	panic("n/a")
}

// Empty checks array is empty
func (arr *Array) Empty() bool {
	return arr.count == 0
}

// Count returns number of nodes in the arr.
func (arr *Array) Count() int {
	return arr.count
}

// Items returns all items in-order
func (arr *Array) Items() []interface{} {
	items := make([]interface{}, arr.count)
	i := 0
	for iter := arr.First(); !iter.Empty(); iter.Next() {
		items[i] = iter.Item()
		i++
	}
	return items
}

// First returns the left-most (min) node or nil if empty.
func (arr *Array) First() rbtree.OrderedIter {
	if arr.count == 0 {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: 0}
}

// Last returns the right-most (max) node or nil if empty.
func (arr *Array) Last() rbtree.OrderedIter {
	if arr.count == 0 {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: len(arr.nodes) - 1}
}

// Find position by given operation and key
func (arr *Array) Find(op int, key interface{}) (iter rbtree.OrderedIter) {
	switch op {
	case rbtree.EQ:
		iter = arr.FindEq(key)
	case rbtree.GE:
		iter = arr.FindGe(key)
	case rbtree.GT:
		iter = arr.FindGt(key)
	case rbtree.LE:
		iter = arr.FindLe(key)
	case rbtree.LT:
		iter = arr.FindLt(key)
	default:
		panic("op")
	}
	return iter
}

// FindEq finds position, that "=" key
func (arr *Array) FindEq(key interface{}) rbtree.OrderedIter {
	idx := arr.lookup(key)
	if idx < 0 {
		return emptyIter()
	}
	return &Iter{idx: idx, arr: arr}
}

func (arr *Array) lookup(key interface{}) (idx int) {
	for left, right := 0, arr.count; left < right; {
		mid := left + (right-left)/2
		compare := arr.ComparatorWithKey(arr.nodes[mid], key)
		if compare == 0 {
			return mid
		}
		if compare < 0 {
			mid++
			left = mid
		} else {
			right = mid
		}
	}
	return -1
}

// FindGt finds position, that ">" key
func (arr *Array) FindGt(key interface{}) rbtree.OrderedIter {
	if key == nil {
		return arr.First()
	}
	mid := 0
	for left, right := 0, arr.count; left < right; {
		mid = left + (right-left)/2
		compare := arr.ComparatorWithKey(arr.nodes[mid], key)
		if compare <= 0 {
			mid++
			left = mid
		} else {
			right = mid
		}
	}
	if arr.count == 0 || mid >= arr.count {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: mid}
}

// FindGe finds position, that ">=" key
func (arr *Array) FindGe(key interface{}) rbtree.OrderedIter {
	if key == nil {
		return arr.First()
	}
	mid := 0
	for left, right := 0, arr.count; left < right; {
		mid = left + (right-left)/2
		compare := arr.ComparatorWithKey(arr.nodes[mid], key)
		if compare < 0 {
			mid++
			left = mid
		} else {
			right = mid
		}
	}
	if arr.count == 0 || mid >= arr.count {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: mid}
}

// FindLt finds position, that "<" key
func (arr *Array) FindLt(key interface{}) rbtree.OrderedIter {
	if key == nil {
		return arr.Last()
	}
	mid := -1
	for left, right := -1, arr.count-1; left < right; {
		mid = left + (right-left+1)/2
		compare := arr.ComparatorWithKey(arr.nodes[mid], key)
		if compare < 0 {
			left = mid
		} else {
			mid--
			right = mid
		}
	}
	if arr.count == 0 || mid < 0 {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: mid}
}

// FindLe finds position, that "<=" key
func (arr *Array) FindLe(key interface{}) rbtree.OrderedIter {
	if key == nil {
		return arr.Last()
	}
	mid := -1
	for left, right := -1, arr.count-1; left < right; {
		mid = left + (right-left+1)/2
		compare := arr.ComparatorWithKey(arr.nodes[mid], key)
		if compare <= 0 {
			left = mid
		} else {
			mid--
			right = mid
		}
	}
	if arr.count == 0 || mid < 0 {
		return emptyIter()
	}
	return &Iter{arr: arr, idx: mid}
}

// ForEach applies function for each array item
func (arr *Array) ForEach(fn rbtree.ForEachFunc) {
	for iter := arr.First(); !iter.Empty(); iter.Next() {
		cont := fn(iter.Item())
		if !cont {
			break
		}
	}
}

// Clear removes all nodes from the arr.
func (arr *Array) Clear() {
	arr.nodes = nil
	arr.count = 0
}

// String returns a string representation of container
func (arr *Array) String() string {
	str := "sarray = ["
	for iter := arr.First(); !iter.Empty(); iter.Next() {
		str += fmt.Sprintf("%v", iter.Item()) + ","
	}
	str += "]"
	return str
}
