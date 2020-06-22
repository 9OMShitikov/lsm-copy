package sarray

import (
	"github.com/neganovalexey/search/rbtree"
)

// Iter is iterator over ordered array
type Iter struct {
	arr *Array
	idx int
}

func emptyIter() *Iter {
	return &Iter{arr: nil, idx: 0}
}

// Empty checks that is noting to iterate
func (i *Iter) Empty() bool {
	return i.arr == nil
}

// Next iterates to next element
func (i *Iter) Next() bool {
	if i.arr != nil && i.idx < len(i.arr.nodes)-1 {
		i.idx++
		return true
	}
	i.arr = nil
	i.idx = 0
	return false
}

// Prev iterates to previous element
func (i *Iter) Prev() bool {
	if i.arr != nil && i.idx > 0 {
		i.idx--
		return true
	}
	i.arr = nil
	i.idx = 0
	return false
}

// HasNext checks existing of next element
func (i *Iter) HasNext() bool {
	return i.arr != nil && i.idx < len(i.arr.nodes)-1
}

// ForEach applies func for each next element
func (i *Iter) ForEach(f rbtree.ForEachFunc) {
	for ; !i.Empty(); i.Next() {
		if !f(i.Item()) {
			break
		}
	}
}

// Item returns item at current position
func (i *Iter) Item() (item interface{}) {
	if i.arr == nil {
		return nil
	}
	return i.arr.nodes[i.idx]
}

// Remove removes item and current position
func (i *Iter) Remove() {
	panic("n/a")
}
