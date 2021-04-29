package rbtree

import (
	"fmt"
)

type color bool

const (
	black, red color = true, false
)

// Tree holds elements of the red-black tree
type Tree struct {
	Root              *Node
	count             int
	Comparator        Comparator
	ComparatorWithKey Comparator
	ReplaceAction     ReplaceAction
}

// Node is a single element within the tree
type Node struct {
	Item   interface{}
	color  color
	Left   *Node
	Right  *Node
	Parent *Node
}

// nolint:unused, deadcode
func assertInterfaces() {
	var _ OrderedTree = (*Tree)(nil)
	var _ OrderedIter = (*Iter)(nil)
}

// New instantiates a red-black tree with the custom comparator.
func New(comparator Comparator, comparatorWithKey Comparator) *Tree {
	return &Tree{Comparator: comparator, ComparatorWithKey: comparatorWithKey}
}

// NewWithIntComparator instantiates a red-black tree with the IntComparator, i.e. keys are of type int.
func NewWithIntComparator() *Tree {
	return &Tree{Comparator: IntComparator, ComparatorWithKey: IntComparator}
}

// NewWithStringComparator instantiates a red-black tree with the StringComparator, i.e. keys are of type string.
func NewWithStringComparator() *Tree {
	return &Tree{Comparator: StringComparator, ComparatorWithKey: StringComparator}
}

// NewWithReplaceAction a red-black tree with the custom comparator and custom replace action
func NewWithReplaceAction(comparator Comparator, comparatorWithKey Comparator, replaceAction ReplaceAction) *Tree {
	return &Tree{Comparator: comparator, ComparatorWithKey: comparatorWithKey, ReplaceAction: replaceAction}
}

// Insert item to tree (without replacing existing element)
func (tree *Tree) Insert(item interface{}) (existing interface{}) {
	existing, _ = tree.InsertOrReplace(item, false)
	return
}

// InsertOrReplace item to tree with given action (replace or not) on existing element
func (tree *Tree) InsertOrReplace(item interface{}, replace bool) (existing, result interface{}) {
	existing = nil
	insertedNode := &Node{Item: item, color: red}
	if tree.Root == nil {
		tree.Root = insertedNode
	} else {
		node := tree.Root
	loop:
		for {
			compare := tree.Comparator(node.Item, item)
			if compare == 0 {
				existing = node.Item
				if replace {
					result = item
					if tree.ReplaceAction != nil {
						result = tree.ReplaceAction(existing, result)
					}
					node.Item = result
					return existing, result
				}
				// fallthrough to <0 case...
			}

			if compare > 0 {
				if node.Left != nil {
					node = node.Left
				} else {
					node.Left = insertedNode
					break loop
				}
			} else {
				if node.Right != nil {
					node = node.Right
				} else {
					node.Right = insertedNode
					break loop
				}
			}
		}
		insertedNode.Parent = node
	}
	tree.insertCase1(insertedNode)
	tree.count++
	return existing, item
}

// Search the node in the tree by key and returns its value or nil if key is not found in tree.
// Key should adhere to the comparator's type assertion, otherwise method panics.
func (tree *Tree) Search(key interface{}) (item interface{}) {
	node := tree.lookup(key)
	if node != nil {
		return node.Item
	}
	return nil
}

// Remove the node from the tree by key.
// Key should adhere to the comparator's type assertion, otherwise method panics.
func (tree *Tree) Remove(key interface{}) {
	node := tree.lookup(key)
	if node == nil {
		return
	}
	tree.RemoveNode(node)
}

// RemoveItem removes the node from the tree by key.
// Instead of key item is specified compared to Remove()
func (tree *Tree) RemoveItem(item interface{}) {
	node := tree.lookupItem(item)
	if node == nil {
		return
	}
	tree.RemoveNode(node)
}

// RemoveNode removes the given node from the tree.
func (tree *Tree) RemoveNode(node *Node) {
	if node.Left != nil && node.Right != nil {
		pred := node.Left.maximumNode()
		node.Item = pred.Item
		node = pred
	}
	if node.Left == nil || node.Right == nil {
		var child *Node
		if node.Right == nil {
			child = node.Left
		} else {
			child = node.Right
		}
		if node.color == black {
			node.color = nodeColor(child)
			tree.deleteCase1(node)
		}
		tree.replaceNode(node, child)
		if node.Parent == nil && child != nil {
			child.color = black
		}
	}
	tree.count--
}

// Empty returns true if tree does not contain any nodes
func (tree *Tree) Empty() bool {
	return tree.count == 0
}

// Count returns number of nodes in the tree.
func (tree *Tree) Count() int {
	return tree.count
}

// Items returns all items in-order
func (tree *Tree) Items() []interface{} {
	items := make([]interface{}, tree.count)
	i := 0
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		items[i] = iter.Item()
		i++
	}
	return items
}

// First returns the left-most (min) node or nil if tree is empty.
func (tree *Tree) First() OrderedIter {
	node := tree.Root
	if node == nil {
		return &Iter{node: nil, tree: tree}
	}

	for node.Left != nil {
		node = node.Left
	}
	return &Iter{node: node, tree: tree}
}

// Last returns the right-most (max) node or nil if tree is empty.
func (tree *Tree) Last() OrderedIter {
	node := tree.Root
	if node == nil {
		return &Iter{node: nil, tree: tree}
	}

	for node.Right != nil {
		node = node.Right
	}
	return &Iter{node: node, tree: tree}
}

// Next returns the next node in the tree or nil if it is the right-most node
func (node *Node) Next() *Node {
	if node.Right != nil {
		node = node.Right
		/* go left as far as we can */
		for node.Left != nil {
			node = node.Left
		}
		return node
	}
	var parent *Node
	for parent = node.Parent; parent != nil && node == parent.Right; parent = node.Parent {
		node = parent
	}
	return parent
}

// Prev returns the prev node in the tree or nil if it is the left-most node
func (node *Node) Prev() *Node {
	if node.Left != nil {
		node = node.Left
		/* go right as far as we can */
		for node.Right != nil {
			node = node.Right
		}
		return node
	}
	var parent *Node
	for parent = node.Parent; parent != nil && node == parent.Left; parent = node.Parent {
		node = parent
	}
	return parent
}

// Find position according to given operation and key
func (tree *Tree) Find(op int, key interface{}) (iter OrderedIter) {
	switch op {
	case EQ:
		iter = tree.FindEq(key)
	case GE:
		iter = tree.FindGe(key)
	case GT:
		iter = tree.FindGt(key)
	case LE:
		iter = tree.FindLe(key)
	case LT:
		iter = tree.FindLt(key)
	default:
		panic("op")
	}
	return iter
}

// FindEq find position, that equals given key
func (tree *Tree) FindEq(key interface{}) OrderedIter {
	return &Iter{node: tree.lookup(key), tree: tree}
}

// FindGt find position, that ">" given key
func (tree *Tree) FindGt(key interface{}) OrderedIter {
	if key == nil {
		return tree.First()
	}
	var prev *Node
	node := tree.Root
	for node != nil {
		compare := tree.ComparatorWithKey(node.Item, key)
		switch {
		case compare > 0:
			prev = node
			node = node.Left
		case compare <= 0:
			node = node.Right
		}
	}
	return &Iter{node: prev, tree: tree}
}

// FindLt find position, that "<" given key
func (tree *Tree) FindLt(key interface{}) OrderedIter {
	if key == nil {
		return tree.Last()
	}
	var prev *Node
	node := tree.Root
	for node != nil {
		compare := tree.ComparatorWithKey(node.Item, key)
		switch {
		case compare < 0:
			prev = node
			node = node.Right
		case compare >= 0:
			node = node.Left
		}
	}
	return &Iter{node: prev, tree: tree}
}

// FindGe find position, that ">=" given key
func (tree *Tree) FindGe(key interface{}) OrderedIter {
	if key == nil {
		return tree.First()
	}
	var prev *Node
	node := tree.Root
	for node != nil {
		compare := tree.ComparatorWithKey(node.Item, key)
		switch {
		case compare > 0:
			prev = node
			node = node.Left
		case compare == 0:
			return &Iter{node: node, tree: tree}
		case compare < 0:
			node = node.Right
		}
	}
	return &Iter{node: prev, tree: tree}
}

// FindLe find position, that "<=" given key
func (tree *Tree) FindLe(key interface{}) OrderedIter {
	if key == nil {
		return tree.Last()
	}
	var prev *Node
	node := tree.Root
	for node != nil {
		compare := tree.ComparatorWithKey(node.Item, key)
		switch {
		case compare < 0:
			prev = node
			node = node.Right
		case compare == 0:
			return &Iter{node: node, tree: tree}
		case compare > 0:
			node = node.Left
		}
	}
	return &Iter{node: prev, tree: tree}
}

// ForEach calls func for each tree node
func (tree *Tree) ForEach(fn ForEachFunc) {
	for iter := tree.First(); !iter.Empty(); iter.Next() {
		cont := fn(iter.Item())
		if !cont {
			break
		}
	}
}

// Clear removes all nodes from the tree.
func (tree *Tree) Clear() {
	tree.Root = nil
	tree.count = 0
}

// String returns a string representation of container
func (tree *Tree) String() string {
	str := "Rbtree\n"
	if !tree.Empty() {
		output(tree.Root, "", true, &str)
	}
	return str
}

func (node *Node) String() string {
	return fmt.Sprintf("%v", node.Item)
}

func output(node *Node, prefix string, isTail bool, str *string) {
	if node.Right != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "│   "
		} else {
			newPrefix += "    "
		}
		output(node.Right, newPrefix, false, str)
	}
	*str += prefix
	if isTail {
		*str += "└── "
	} else {
		*str += "┌── "
	}
	*str += node.String() + "\n"
	if node.Left != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "    "
		} else {
			newPrefix += "│   "
		}
		output(node.Left, newPrefix, true, str)
	}
}

func (tree *Tree) lookup(key interface{}) *Node {
	node := tree.Root
	for node != nil {
		compare := tree.ComparatorWithKey(node.Item, key)
		switch {
		case compare == 0:
			return node
		case compare > 0:
			node = node.Left
		case compare < 0:
			node = node.Right
		}
	}
	return nil
}

func (tree *Tree) lookupItem(item interface{}) *Node {
	node := tree.Root
	for node != nil {
		compare := tree.Comparator(node.Item, item)
		switch {
		case compare == 0:
			return node
		case compare > 0:
			node = node.Left
		case compare < 0:
			node = node.Right
		}
	}
	return nil
}

func (node *Node) grandparent() *Node {
	if node != nil && node.Parent != nil {
		return node.Parent.Parent
	}
	return nil
}

func (node *Node) uncle() *Node {
	if node == nil || node.Parent == nil || node.Parent.Parent == nil {
		return nil
	}
	return node.Parent.sibling()
}

func (node *Node) sibling() *Node {
	if node == nil || node.Parent == nil {
		return nil
	}
	if node == node.Parent.Left {
		return node.Parent.Right
	}
	return node.Parent.Left
}

func (tree *Tree) rotateLeft(node *Node) {
	right := node.Right
	tree.replaceNode(node, right)
	node.Right = right.Left
	if right.Left != nil {
		right.Left.Parent = node
	}
	right.Left = node
	node.Parent = right
}

func (tree *Tree) rotateRight(node *Node) {
	left := node.Left
	tree.replaceNode(node, left)
	node.Left = left.Right
	if left.Right != nil {
		left.Right.Parent = node
	}
	left.Right = node
	node.Parent = left
}

func (tree *Tree) replaceNode(old *Node, new *Node) {
	if old.Parent == nil {
		tree.Root = new
	} else {
		if old == old.Parent.Left {
			old.Parent.Left = new
		} else {
			old.Parent.Right = new
		}
	}
	if new != nil {
		new.Parent = old.Parent
	}
}

func (tree *Tree) insertCase1(node *Node) {
	if node.Parent == nil {
		node.color = black
	} else {
		tree.insertCase2(node)
	}
}

func (tree *Tree) insertCase2(node *Node) {
	if nodeColor(node.Parent) == black {
		return
	}
	tree.insertCase3(node)
}

func (tree *Tree) insertCase3(node *Node) {
	uncle := node.uncle()
	if nodeColor(uncle) == red {
		node.Parent.color = black
		uncle.color = black
		node.grandparent().color = red
		tree.insertCase1(node.grandparent())
	} else {
		tree.insertCase4(node)
	}
}

func (tree *Tree) insertCase4(node *Node) {
	grandparent := node.grandparent()
	if node == node.Parent.Right && node.Parent == grandparent.Left {
		tree.rotateLeft(node.Parent)
		node = node.Left
	} else if node == node.Parent.Left && node.Parent == grandparent.Right {
		tree.rotateRight(node.Parent)
		node = node.Right
	}
	tree.insertCase5(node)
}

func (tree *Tree) insertCase5(node *Node) {
	node.Parent.color = black
	grandparent := node.grandparent()
	grandparent.color = red
	if node == node.Parent.Left && node.Parent == grandparent.Left {
		tree.rotateRight(grandparent)
	} else if node == node.Parent.Right && node.Parent == grandparent.Right {
		tree.rotateLeft(grandparent)
	}
}

func (node *Node) maximumNode() *Node {
	if node == nil {
		return nil
	}
	for node.Right != nil {
		node = node.Right
	}
	return node
}

func (tree *Tree) deleteCase1(node *Node) {
	if node.Parent == nil {
		return
	}
	tree.deleteCase2(node)
}

func (tree *Tree) deleteCase2(node *Node) {
	sibling := node.sibling()
	if nodeColor(sibling) == red {
		node.Parent.color = red
		sibling.color = black
		if node == node.Parent.Left {
			tree.rotateLeft(node.Parent)
		} else {
			tree.rotateRight(node.Parent)
		}
	}
	tree.deleteCase3(node)
}

func (tree *Tree) deleteCase3(node *Node) {
	sibling := node.sibling()
	if nodeColor(node.Parent) == black &&
		nodeColor(sibling) == black &&
		nodeColor(sibling.Left) == black &&
		nodeColor(sibling.Right) == black {
		sibling.color = red
		tree.deleteCase1(node.Parent)
	} else {
		tree.deleteCase4(node)
	}
}

func (tree *Tree) deleteCase4(node *Node) {
	sibling := node.sibling()
	if nodeColor(node.Parent) == red &&
		nodeColor(sibling) == black &&
		nodeColor(sibling.Left) == black &&
		nodeColor(sibling.Right) == black {
		sibling.color = red
		node.Parent.color = black
	} else {
		tree.deleteCase5(node)
	}
}

func (tree *Tree) deleteCase5(node *Node) {
	sibling := node.sibling()
	if node == node.Parent.Left &&
		nodeColor(sibling) == black &&
		nodeColor(sibling.Left) == red &&
		nodeColor(sibling.Right) == black {
		sibling.color = red
		sibling.Left.color = black
		tree.rotateRight(sibling)
	} else if node == node.Parent.Right &&
		nodeColor(sibling) == black &&
		nodeColor(sibling.Right) == red &&
		nodeColor(sibling.Left) == black {
		sibling.color = red
		sibling.Right.color = black
		tree.rotateLeft(sibling)
	}
	tree.deleteCase6(node)
}

func (tree *Tree) deleteCase6(node *Node) {
	sibling := node.sibling()
	sibling.color = nodeColor(node.Parent)
	node.Parent.color = black
	if node == node.Parent.Left && nodeColor(sibling.Right) == red {
		sibling.Right.color = black
		tree.rotateLeft(node.Parent)
	} else if nodeColor(sibling.Left) == red {
		sibling.Left.color = black
		tree.rotateRight(node.Parent)
	}
}

func nodeColor(node *Node) color {
	if node == nil {
		return black
	}
	return node.color
}
