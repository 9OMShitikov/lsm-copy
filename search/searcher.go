package search

import (
	"github.com/RoaringBitmap/roaring"

	"github.com/neganovalexey/search/lsm"
)

// FeatureID + fieldID + blockID is a key in an index
type (
	// FeatureID is a data type for feature ID
	FeatureID = uint64
	// FieldID is a data type for field ID
	FieldID = uint16
	// BlockID is a data type for block ID
	BlockID = uint32
	// FieldType is a data type for field type
	FieldType = uint8
)

// field types
const (
	_ = FieldType(iota)
	// FieldTypeDateTime is a type of datetime fields
	FieldTypeDateTime
	// FieldTypeExactTerms is a type for exact word matching
	FieldTypeExactTerms
)

// BlockIterator allows to iterate over bitmap blocks for given feature & field IDs
type BlockIterator interface {
	Empty() bool
	Next()
	Result() (FeatureID, FieldID, BlockID, *roaring.Bitmap)
	Error() error
}

// Find orders
const (
	EQ = lsm.EQ
	GT = lsm.GT
	GE = lsm.GE
	LT = lsm.LT
	LE = lsm.LE
)

// Searcher represents a mockable bitmap index
// The index can be logically represented as following:
//           |       block1       |       block2       | ...
//           | doc1 | doc2 | doc3 | doc4 | doc5 | doc6 | ...
// feature1  |  0   |  0   |  1   |  1   |  1   |  1   | ...
// feature2  |  1   |  0   |  1   |  0   |  1   |  1   | ...
type Searcher interface {
	// Search for exact block
	Search(FeatureID, FieldID, BlockID) (*roaring.Bitmap, error)
	// Find blocks with given feature & field id, starting from 'from' block in 'op' order
	Find(op int, feature FeatureID, field FieldID, from BlockID) BlockIterator
	// Insert adds an element to the index
	Insert(feature FeatureID, field FieldID, docID uint64) error
	// GetBlockBitmap returns a bitmap for present blocks of feature&field id
	GetBlockBitmap(FeatureID, FieldID) (*roaring.Bitmap, error)
	// BitmapBlockSize returns the index's block size so that document ID can be calculated
	BitmapBlockSize() uint32

	ReadDesc(buf *lsm.DeserializeBuf) error
	WriteDesc(buf *lsm.SerializeBuf)

	// WaitAllBackgroundInsertionRoutines waits until all background insertion procedures not complete.
	// Should be called before WriteDesc
	WaitAllBackgroundInsertionRoutines()
}
