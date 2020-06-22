package rbtree

// Iter is iterator over red-black tree
type Iter struct {
	tree *Tree
	node *Node
}

// Empty checks is nothing to iterate
func (i *Iter) Empty() bool {
	return i.node == nil
}

// Next iterates to next element
func (i *Iter) Next() bool {
	i.node = i.node.Next()
	return i.node != nil
}

// Prev iterates to previous element
func (i *Iter) Prev() bool {
	i.node = i.node.Prev()
	return i.node != nil
}

// HasNext checks has next element
func (i *Iter) HasNext() bool {
	return i.node.Next() != nil
}

// ForEach applies for each next item
func (i *Iter) ForEach(f ForEachFunc) {
	for ; !i.Empty(); i.Next() {
		if !f(i.node.Item) {
			break
		}
	}
}

// Item return item, that is assigned to current iterator position
func (i *Iter) Item() (item interface{}) {
	return i.node.Item
}

// Remove node at current position and go to next
func (i *Iter) Remove() {
	node := i.node
	i.node = i.node.Next()
	i.tree.RemoveNode(node)
}
