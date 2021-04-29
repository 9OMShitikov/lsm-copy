package search_test

import (
	"hash/crc32"

	"github.com/RoaringBitmap/roaring"

	"github.com/neganovalexey/search/codeerrors"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/search"
	"github.com/neganovalexey/search/search/dict"
	"github.com/neganovalexey/search/search/docmap"
)

// ---------------------------------------------------------------------------------------------------------------------
// mock of BitLsm
// ---------------------------------------------------------------------------------------------------------------------
var _ search.Searcher = (*testSearcher)(nil)

type testSearcherKey struct {
	feature search.FeatureID
	field   search.FieldID
	block   search.BlockID
}

type testSearcher struct {
	bitmaps    map[testSearcherKey]*roaring.Bitmap
	maxBlockID search.BlockID
}

func (s *testSearcher) Search(feature search.FeatureID, field search.FieldID, block search.BlockID) (*roaring.Bitmap, error) {
	return s.bitmaps[testSearcherKey{feature: feature, field: field, block: block}], nil
}

func (s *testSearcher) WaitAllBackgroundInsertionRoutines() {}

type testBlock struct {
	blockID search.BlockID
	bitmap  *roaring.Bitmap
}

type testBlockIterator struct {
	feature search.FeatureID
	field   search.FieldID
	blocks  []testBlock
}

func (it *testBlockIterator) Empty() bool {
	return len(it.blocks) == 0
}

func (it *testBlockIterator) Error() error {
	return nil
}

func (it *testBlockIterator) Next() {
	it.blocks = it.blocks[1:]
}

func (it *testBlockIterator) Result() (search.FeatureID, search.FieldID, search.BlockID, *roaring.Bitmap) {
	return it.feature, it.field, it.blocks[0].blockID, it.blocks[0].bitmap
}

func (s *testSearcher) Find(op int, feature search.FeatureID, field search.FieldID, from search.BlockID) search.BlockIterator {
	fromX := int64(from)
	var blocks []testBlock
	switch op {
	case search.EQ:
		bitmap, ok := s.bitmaps[testSearcherKey{feature: feature, field: field, block: from}]
		if ok {
			blocks = []testBlock{{blockID: from, bitmap: bitmap}}
		}
	case search.GT:
		from++
		fallthrough
	case search.GE:
		for blockID := from; blockID <= s.maxBlockID; blockID++ {
			bitmap, ok := s.bitmaps[testSearcherKey{feature: feature, field: field, block: blockID}]
			if ok {
				blocks = append(blocks, testBlock{blockID: blockID, bitmap: bitmap})
			}
		}
	case search.LT:
		fromX--
		fallthrough
	case search.LE:
		for blockID := fromX; blockID >= 0; blockID-- {
			bitmap, ok := s.bitmaps[testSearcherKey{feature: feature, field: field, block: search.BlockID(blockID)}]
			if ok {
				blocks = append(blocks, testBlock{blockID: search.BlockID(blockID), bitmap: bitmap})
			}
		}
	default:
		panic("invalid operation")
	}
	return &testBlockIterator{
		feature: feature,
		field:   field,
		blocks:  blocks,
	}
}

func (s *testSearcher) GetBlockBitmap(feature search.FeatureID, field search.FieldID) (*roaring.Bitmap, error) {
	result := roaring.New()
	for blockID := search.BlockID(0); blockID <= s.maxBlockID; blockID++ {
		_, ok := s.bitmaps[testSearcherKey{feature: feature, field: field, block: blockID}]
		if ok {
			result.Add(blockID)
		}
	}
	return result, nil
}

func (s *testSearcher) BitmapBlockSize() uint32 {
	return 4
}

func (s *testSearcher) Insert(feature search.FeatureID, field search.FieldID, docID uint64) error {
	blockID, bit := search.BlockID(docID/uint64(s.BitmapBlockSize())), uint32(docID%uint64(s.BitmapBlockSize()))
	if blockID > s.maxBlockID {
		s.maxBlockID = blockID
	}

	key := testSearcherKey{feature: feature, field: field, block: blockID}
	bitmap, ok := s.bitmaps[key]
	if !ok {
		bitmap = roaring.New()
	}
	bitmap.Add(bit)
	s.bitmaps[key] = bitmap

	return nil
}

func (*testSearcher) WriteDesc(*lsm.SerializeBuf)            {}
func (*testSearcher) ReadDesc(*lsm.DeserializeBuf) (_ error) { return }

// ---------------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------------------------
// mock of Dictionary
// ---------------------------------------------------------------------------------------------------------------------
var _ dict.Dictionary = (*testDictionary)(nil)

type testDictionary struct {
	terms map[dict.Term]search.FeatureID
}

func (d *testDictionary) GetOrInsert(fieldID uint16, term dict.Term) (search.FeatureID, error) {
	termID := search.FeatureID(crc32.ChecksumIEEE([]byte(term)))
	d.terms[term] = termID
	return termID, nil
}

func (d *testDictionary) Get(fieldID uint16, term dict.Term) (search.FeatureID, error) {
	termID, ok := d.terms[term]
	if !ok {
		return 0, codeerrors.ErrNotFound
	}
	return termID, nil
}

func (*testDictionary) WriteDesc(*lsm.SerializeBuf)            {}
func (*testDictionary) ReadDesc(*lsm.DeserializeBuf) (_ error) { return }
func (*testDictionary) WaitAllBackgroundInsertionRoutines()    {}

// ---------------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------------------------
// mock of DocMap
// ---------------------------------------------------------------------------------------------------------------------
var _ docmap.DocMap = (*testArchiveMapping)(nil)

type testArchiveMapping struct {
	doc2inodePit map[uint64]search.DocID
}

func (m *testArchiveMapping) Insert(docID, inodeID, pitID uint64) (err error) {
	m.doc2inodePit[docID] = search.DocID{InodeID: inodeID, PitID: pitID}
	return nil
}

func (m *testArchiveMapping) LookupInodeVersion(docID uint64) (inodeID, pitID uint64, err error) {
	result, ok := m.doc2inodePit[docID]
	if !ok {
		return 0, 0, docmap.ErrDocNotFound
	}
	return result.InodeID, result.PitID, nil
}

func (*testArchiveMapping) WriteDesc(*lsm.SerializeBuf)            {}
func (*testArchiveMapping) ReadDesc(*lsm.DeserializeBuf) (_ error) { return }
func (*testArchiveMapping) WaitAllBackgroundInsertionRoutines()    {}

// ---------------------------------------------------------------------------------------------------------------------
