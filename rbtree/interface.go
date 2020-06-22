package rbtree

// ForEachFunc is func, that runs for each item
type ForEachFunc func(item interface{}) bool

const (
	// EQ is "="
	EQ = iota
	// GE is ">="
	GE
	// GT is ">"
	GT
	// LE is "<="
	LE
	// LT is "<"
	LT
)

// OrderedTree is interface that described common ordered tree interface
type OrderedTree interface {
	Empty() bool
	Count() int
	Clear()

	Insert(item interface{}) (existing interface{})
	Search(key interface{}) (item interface{})
	Remove(key interface{})

	Items() []interface{}

	First() OrderedIter
	Last() OrderedIter

	Find(op int, key interface{}) OrderedIter
	FindEq(key interface{}) OrderedIter
	FindGt(key interface{}) OrderedIter
	FindLt(key interface{}) OrderedIter
	FindGe(key interface{}) OrderedIter
	FindLe(key interface{}) OrderedIter

	ForEach(f ForEachFunc)
}

// OrderedIter is interface for iteration over ordered tree
type OrderedIter interface {
	Item() (item interface{})

	Empty() bool
	Next() bool
	Prev() bool

	HasNext() bool

	// iterate starting from current pos
	ForEach(f ForEachFunc)

	// remove current element
	Remove()
}
