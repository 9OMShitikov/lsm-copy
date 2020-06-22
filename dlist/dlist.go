package dlist

// ListHead is head of the list
type ListHead struct {
	List
	e2obj List2ObjFunc
}

// List2ObjFunc converts list element to specific interface
type List2ObjFunc func(el *List) interface{}

// List is double-linked list
type List struct {
	next, prev *List
}

// Init initializes empty list
func (h *ListHead) Init(fn List2ObjFunc) {
	h.e2obj = fn
	h.next = &h.List
	h.prev = &h.List
}

// Add element to list head
func (h *ListHead) Add(el *List) {
	next := h.next
	h.next = el
	el.prev = &h.List
	el.next = next
	next.prev = el
}

// AddTail adds element to list tail
func (h *ListHead) AddTail(el *List) {
	prev := h.prev
	h.prev = el
	el.next = &h.List
	el.prev = prev
	prev.next = el
}

// Move element to list head
func (h *ListHead) Move(el *List) {
	el.Remove()
	h.Add(el)
}

// MoveTail moves element to list tail
func (h *ListHead) MoveTail(el *List) {
	el.Remove()
	h.AddTail(el)
}

// Splice oldh list to h list
func (h *ListHead) Splice(oldh *ListHead) {
	if oldh.Empty() {
		return
	}
	next := h.next
	h.next = oldh.next
	oldh.next.prev = &h.List
	oldh.prev.next = next
	next.prev = oldh.prev

	oldh.Init(oldh.e2obj)
}

// SpliceTail splices oldh list to h list
func (h *ListHead) SpliceTail(oldh *ListHead) {
	if oldh.Empty() {
		return
	}
	prev := h.prev
	h.prev = oldh.prev
	oldh.prev.next = &h.List
	oldh.next.prev = prev
	prev.next = oldh.next

	oldh.Init(oldh.e2obj)
}

// Empty checks list empty
func (h *ListHead) Empty() bool {
	return h.next == &h.List
}

// First returns list head
func (h *ListHead) First() *List {
	if h.next != &h.List {
		return h.next
	}
	return nil
}

// Last returns list tail
func (h *ListHead) Last() *List {
	if h.prev != &h.List {
		return h.prev
	}
	return nil
}

// Next returns next element
func (h *ListHead) Next(el *List) *List {
	if el.next == &h.List {
		return nil
	}
	return el.next
}

// Prev returns prev element
func (h *ListHead) Prev(el *List) *List {
	if el.prev == &h.List {
		return nil
	}
	return el.prev
}

// Remove element from list
func (el *List) Remove() {
	if el.next == nil && el.prev == nil {
		return
	}
	prev, next := el.prev, el.next
	prev.next = next
	next.prev = prev
	el.prev, el.next = nil, nil
}

// Inserted checks elements is in list
func (el *List) Inserted() bool {
	return el.next != nil
}

// IsHead checks elements is list head
func (el *List) IsHead(h *ListHead) bool {
	return el == &h.List
}

// ForEach is safe vs. element remove in callback
func (h *ListHead) ForEach(f func(*List) bool) {
	var next *List
	for el := h.First(); el != nil; el = next {
		next = h.Next(el)
		if !f(el) {
			break
		}
	}
}

// Items returns list items
func (h *ListHead) Items() []interface{} {
	items := make([]interface{}, 0)
	for el := h.First(); el != nil; el = h.Next(el) {
		items = append(items, h.e2obj(el))
	}
	return items
}
