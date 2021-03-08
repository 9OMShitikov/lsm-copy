package search

import (
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"

	"github.com/neganovalexey/search/search/datetime"
)

// DateRange represents time range for some field, i. e. document creation time
type DateRange struct {
	From time.Time
	To   time.Time
}

type dateTimeRangeReader struct {
	field FieldID
	dnf   []datetime.DateRangeCode
}

func dateTimeToFeature(order, year int) FeatureID {
	y := FeatureID(year)
	if y < 1900 {
		y = 1900
	} else if y > 2155 {
		y = 2155
	}
	y -= 1900

	return FeatureID(order)<<16 + y
}

func (dr dateTimeRangeReader) GetBlockBitmap(s Searcher) (*roaring.Bitmap, error) {
	toOr := make([]*roaring.Bitmap, 0, len(dr.dnf))
loopOr:
	for _, conj := range dr.dnf {
		featureID := dateTimeToFeature(0, conj.Year)
		bitmap, err := s.GetBlockBitmap(featureID, dr.field)
		if bitmap == nil {
			continue loopOr
		} else if err != nil {
			return nil, err
		}
	loopAnd:
		for order, sign := range conj.InYearCode {
			if sign <= 0 {
				continue loopAnd
			}
			featureID := dateTimeToFeature(order+1, 0)
			feature, err := s.GetBlockBitmap(featureID, dr.field)
			if err != nil {
				return nil, err
			} else if feature == nil {
				// x OR (y AND 0) = x OR 0 = x
				continue loopOr
			}
			bitmap.And(feature)
		}
		toOr = append(toOr, bitmap)
	}
	return roaring.FastOr(toOr...), nil
}

func (dr dateTimeRangeReader) GetBlock(s Searcher, block BlockID) (*roaring.Bitmap, error) {
	toOr := make([]*roaring.Bitmap, 0, len(dr.dnf))
loopOr:
	for _, conj := range dr.dnf {
		featureID := dateTimeToFeature(0, conj.Year)
		bitmap, err := s.Search(featureID, dr.field, block)
		if bitmap == nil {
			continue loopOr
		} else if err != nil {
			return nil, err
		}
	loopAnd:
		for order, sign := range conj.InYearCode {
			featureID := dateTimeToFeature(order+1, 0)
			feature, err := s.Search(featureID, dr.field, block)
			if err != nil {
				return nil, err
			} else if feature == nil && sign > 0 {
				// x OR (y AND 0) = x OR 0 = x
				continue loopOr
			} else if feature == nil {
				// x AND NOT 0 == x AND 1 = x
				continue loopAnd
			}
			if sign > 0 {
				bitmap.And(feature)
			} else {
				bitmap.AndNot(feature)
			}
		}
		toOr = append(toOr, bitmap)
	}
	return roaring.FastOr(toOr...), nil
}

func getDateTimeRangeReader(fieldID FieldID, value interface{}) (featureFieldReader, error) {
	dtRange, ok := value.(DateRange)
	if !ok {
		return nil, errors.Errorf("wrong value type: %T", value)
	}
	codes := datetime.ParseTimeRange(dtRange.From, dtRange.To)

	return dateTimeRangeReader{field: fieldID, dnf: codes}, nil
}

type dateTimeWriter struct {
	field FieldID
}

func (dw dateTimeWriter) Insert(s Searcher, docID uint64, val interface{}) error {
	t, ok := val.(time.Time)
	if !ok {
		return errors.Errorf("wrong value type %T for dateTimeWriter", val)
	}
	year, orders := datetime.ParseTime(t)

	features := []FeatureID{dateTimeToFeature(0, year)}
	for _, order := range orders {
		features = append(features, dateTimeToFeature(order, 0))
	}

	for _, feature := range features {
		if err := s.Insert(feature, dw.field, docID); err != nil {
			return err
		}
	}

	return nil
}

func newDateTimeWriter(field FieldID) featureFieldWriter {
	return dateTimeWriter{field: field}
}
