package search

import (
	"github.com/RoaringBitmap/roaring"

	"github.com/neganovalexey/search/search/dict"
	"github.com/pkg/errors"
)

type termFeatureFieldReader struct {
	feature FeatureID
	field   FieldID
}

func (t termFeatureFieldReader) GetBlockBitmap(s Searcher) (*roaring.Bitmap, error) {
	return s.GetBlockBitmap(t.feature, t.field)
}

func (t termFeatureFieldReader) GetBlock(s Searcher, block BlockID) (*roaring.Bitmap, error) {
	return s.Search(t.feature, t.field, block)
}

func getTermReader(dictionary dict.Dictionary, fieldID FieldID, value interface{}) (featureFieldReader, error) {
	term, ok := value.(dict.Term)
	if !ok {
		return nil, errors.Errorf("wrong value type: %T", value)
	}
	termID, err := dictionary.Get(fieldID, term)
	if err != nil {
		return nil, err
	}
	return termFeatureFieldReader{feature: termID, field: fieldID}, nil
}

type termFeatureFieldWriter struct {
	dict  dict.Dictionary
	field FieldID
}

func (t termFeatureFieldWriter) Insert(s Searcher, docID uint64, val interface{}) error {
	terms, ok := val.([]dict.Term)
	if !ok {
		return errors.Errorf("wrong value type %T for termFeatureFieldWriter", val)
	}
	for _, term := range terms {
		termID, err := t.dict.GetOrInsert(t.field, term)
		if err != nil {
			return err
		}
		if err = s.Insert(termID, t.field, docID); err != nil {
			return err
		}
	}
	return nil
}

func newTermWriter(dict dict.Dictionary, fieldID FieldID) featureFieldWriter {
	return termFeatureFieldWriter{dict: dict, field: fieldID}
}
