package dlist

import (
	"testing"
	"unsafe"
)

type Obj struct {
	val   string
	list1 List
	list2 List
}

func (el *List) list1ToObj() (o *Obj) {
	return (*Obj)(unsafe.Pointer((uintptr(unsafe.Pointer(el)) - unsafe.Offsetof(o.list1))))
}

func list1ToIntf(el *List) interface{} {
	return el.list1ToObj()
}

func (el *List) list2ToObj() (o *Obj) {
	return (*Obj)(unsafe.Pointer((uintptr(unsafe.Pointer(el)) - unsafe.Offsetof(o.list2))))
}

// add same object to multiple lists
func TestList(t *testing.T) {
	var h1 ListHead
	var h2 ListHead
	h1.Init(nil)
	h2.Init(nil)

	o1 := &Obj{val: "1"}
	o2 := &Obj{val: "2"}

	h1.Add(&o1.list1)
	h1.AddTail(&o2.list1)
	h2.Add(&o1.list2)

	s := ""
	for e := h1.First(); e != nil; e = h1.Next(e) {
		o := e.list1ToObj()
		s = s + o.val
	}
	if s != "12" {
		t.Fatal(s)
	}

	s = ""
	for e := h2.First(); e != nil; e = h2.Next(e) {
		o := e.list2ToObj()
		s = s + o.val
	}
	if s != "1" {
		t.Fatal(s)
	}
}

// add/remove object to/from list
func TestList2(t *testing.T) {
	var h ListHead
	h.Init(list1ToIntf)

	if !h.Empty() {
		t.Fatal()
	}

	o1 := &Obj{val: "1"}
	o2 := &Obj{val: "2"}
	o3 := &Obj{val: "3"}
	o4 := &Obj{val: "4"}
	o5 := &Obj{val: "5"}

	h.AddTail(&o2.list1)
	h.AddTail(&o3.list1)
	h.AddTail(&o4.list1)
	h.AddTail(&o5.list1)
	h.Add(&o1.list1)

	h.MoveTail(&o1.list1)
	o2.list1.Remove()
	h.ForEach(func(e *List) bool {
		o := e.list1ToObj()
		if o.val == "3" {
			o.list1.Remove()
		}
		return true
	})
	s := ""
	h.ForEach(func(e *List) bool { s += e.list1ToObj().val; return true })
	if s != "451" {
		t.Fatal(s)
	}
	if h.Empty() {
		t.Fatal()
	}
	o1.list1.Remove()
	o5.list1.Remove()
	if h.Empty() {
		t.Fatal()
	}

	o4.list1.Remove()
	if !h.Empty() {
		t.Fatal()
	}
}

// test splicing
func TestListSplice1(t *testing.T) {
	var h1 ListHead
	var h2 ListHead
	h1.Init(list1ToIntf)
	h2.Init(list1ToIntf)

	o1 := &Obj{val: "1"}
	o2 := &Obj{val: "2"}
	o3 := &Obj{val: "3"}
	o4 := &Obj{val: "4"}

	h1.AddTail(&o1.list1)
	h1.AddTail(&o2.list1)
	h2.AddTail(&o3.list1)
	h2.AddTail(&o4.list1)

	h1.SpliceTail(&h2)
	if !h2.Empty() {
		t.Fatal()
	}

	// test empty slicing as well
	h1.SpliceTail(&h2)

	s := ""
	h1.ForEach(func(e *List) bool { s += e.list1ToObj().val; return true })
	if s != "1234" {
		t.Fatal(s)
	}
}

// test splicing
func TestListSplice2(t *testing.T) {
	var h1 ListHead
	var h2 ListHead
	h1.Init(list1ToIntf)
	h2.Init(list1ToIntf)

	o1 := &Obj{val: "1"}
	o2 := &Obj{val: "2"}
	o3 := &Obj{val: "3"}
	o4 := &Obj{val: "4"}

	h1.AddTail(&o1.list1)
	h1.AddTail(&o2.list1)
	h2.AddTail(&o3.list1)
	h2.AddTail(&o4.list1)

	h1.Splice(&h2)
	if !h2.Empty() {
		t.Fatal()
	}

	// test empty list splicing as well
	h1.Splice(&h2)

	s := ""
	h1.ForEach(func(e *List) bool { s += e.list1ToObj().val; return true })
	if s != "3412" {
		t.Fatal(s)
	}
}
