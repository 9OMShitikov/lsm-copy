package lsm

import (
	"hash/crc32"

	"github.com/neganovalexey/search/rbtree"
)

// ---------------------------------------------------------------------------
// debug for a common mistake when object returned by LSM is modified by caller
// or if object inserted to LSM is modified after some time.
// RULE: all LSM objects should be immutable. To mutate them - need to clone.
// ---------------------------------------------------------------------------

// verifyLsmNodesImmutable should be used only for testing/debugging (see comment above)
var verifyLsmNodesImmutable = false

func stateCsum(items rbtree.OrderedTree) (csum uint32) {
	if !verifyLsmNodesImmutable {
		return
	}

	for it := items.First(); !it.Empty(); it.Next() {
		buf := NewSerializeBuf(1024)
		it.Item().(Entry).Encode(buf, nil)
		csum ^= crc32.Checksum(buf.Bytes(), crcTab)
	}
	return
}

func stateAdd(csum *uint32, entry Entry) {
	if !verifyLsmNodesImmutable {
		return
	}

	buf := NewSerializeBuf(1024)
	entry.Encode(buf, nil)
	*csum = *csum ^ crc32.Checksum(buf.Bytes(), crcTab)
}

func stateRemove(csum *uint32, entry Entry) {
	stateAdd(csum, entry)
}

// VerifyDecoded verifies that ctreeNode item state is consistent, i.e. items not changed.
// called when cache drops the node and its decoded values or before writing LSM desc.
func VerifyDecoded(data interface{}) {
	if !verifyLsmNodesImmutable || data == nil {
		return
	}

	node, ok := data.(*ctreeNode)
	if !ok {
		return
	}

	if node.isDir {
		return
	}
	// This verifies that cached & decoded node values were not mofidied.
	// i.e. protect from typical error when value returned by Lsm.Find() is modified by user.
	assert(node.stateCsum == stateCsum(node.items))
}
