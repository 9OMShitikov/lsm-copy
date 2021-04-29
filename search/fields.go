package search

import (
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"

	"github.com/neganovalexey/search/codeerrors"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/search/dict"
)

type featureFieldReader interface {
	GetBlockBitmap(Searcher) (*roaring.Bitmap, error)
	GetBlock(Searcher, BlockID) (*roaring.Bitmap, error)
}

type featureFieldWriter interface {
	Insert(s Searcher, docID uint64, value interface{}) error
}

type fieldDesc struct {
	ID   FieldID
	Type FieldType
}

// FeatureFields manages search fields
type FeatureFields struct {
	fieldsByDocumentType map[string]map[string]fieldDesc
	dict                 dict.Dictionary
	lastFieldID          uint16
}

// NewFeatureFields instantiates FeatureFields
func NewFeatureFields(dict dict.Dictionary) (*FeatureFields, error) {
	fields := &FeatureFields{
		dict:                 dict,
		fieldsByDocumentType: make(map[string]map[string]fieldDesc),
	}
	return fields, nil
}

// getReader acquires feature ID for given field ID & payload
func (ff *FeatureFields) getReader(subj FeatureDesc) (featureFieldReader, error) {
	fields, ok := ff.fieldsByDocumentType[subj.DocumentType]
	if !ok {
		return nil, codeerrors.ErrNotFound.WithMessage("unknown document type: %s", subj.DocumentType)
	}
	field, ok := fields[subj.FieldName]
	if !ok {
		return nil, codeerrors.ErrNotFound.WithMessage("unknown field: %s", subj.FieldName)
	}

	switch field.Type {
	case FieldTypeExactTerms:
		return getTermReader(ff.dict, field.ID, subj.Value)
	case FieldTypeDateTime:
		return getDateTimeRangeReader(field.ID, subj.Value)
	default:
		return nil, errors.Errorf("bad field type: %d", field.Type)
	}
}

// GetOrInsert acquires feature ID for given field ID & payload, adding term to the
// underlying term dictionary if necessary
func (ff *FeatureFields) getWriter(documentType, fieldName string) (featureFieldWriter, error) {
	fields, ok := ff.fieldsByDocumentType[documentType]
	if !ok {
		return nil, codeerrors.ErrNotFound.WithMessage("unknown document type: %s", documentType)
	}
	field, ok := fields[fieldName]
	if !ok {
		return nil, codeerrors.ErrNotFound.WithMessage("unknown field: %s", documentType)
	}
	switch field.Type {
	case FieldTypeExactTerms:
		return newTermWriter(ff.dict, field.ID), nil
	case FieldTypeDateTime:
		return newDateTimeWriter(field.ID), nil
	default:
		return nil, errors.Errorf("unknown field type %d", field.Type)
	}
}

// RegisterFieldIfNeeded gets existing field ID or saves info for the new one
func (ff *FeatureFields) RegisterFieldIfNeeded(documentType string, name string, fieldType FieldType) error {
	fields, ok := ff.fieldsByDocumentType[documentType]
	if !ok {
		fields = make(map[string]fieldDesc)
		ff.fieldsByDocumentType[documentType] = fields
	}
	field, ok := fields[name]
	if ok {
		if field.Type != fieldType {
			return codeerrors.ErrConflict.WithMessage("field %s was already registered with type %d", name, field.Type)
		}
		return nil
	}

	if ff.lastFieldID == math.MaxUint16 {
		return codeerrors.ErrOutOfRange.WithMessage("too many fields")
	}
	ff.lastFieldID++

	fields[name] = fieldDesc{ID: ff.lastFieldID, Type: fieldType}
	return nil
}

// WriteDesc serializes FeatureFields
func (ff *FeatureFields) WriteDesc(buf *lsm.SerializeBuf) {
	buf.EncodeUint32(uint32(ff.lastFieldID))
	buf.EncodeUint32(uint32(len(ff.fieldsByDocumentType)))
	for documentType, fields := range ff.fieldsByDocumentType {
		buf.EncodeStr(documentType)
		buf.EncodeUint32(uint32(len(fields)))
		for name, desc := range fields {
			buf.EncodeStr(name)
			buf.EncodeFixedUint16(desc.ID)
			buf.EncodeFixedUint8(desc.Type)
		}
	}
	ff.dict.WriteDesc(buf)
}

// ReadDesc deserializes FeatureFields
func (ff *FeatureFields) ReadDesc(buf *lsm.DeserializeBuf) (err error) {
	lastFieldID := buf.DecodeUint32()
	if lastFieldID > math.MaxUint16 {
		return errors.Errorf("too large lastFieldId")
	}
	ff.lastFieldID = uint16(lastFieldID)
	l := buf.DecodeUint32()
	ff.fieldsByDocumentType = make(map[string]map[string]fieldDesc, int(l))
	for i := 0; i < int(l); i++ {
		documentType := buf.DecodeStr()
		dl := buf.DecodeUint32()
		fields := make(map[string]fieldDesc, int(dl))
		for j := 0; j < int(dl); j++ {
			name := buf.DecodeStr()
			id := buf.DecodeFixedUint16()
			typ := buf.DecodeFixedUint8()
			fields[name] = fieldDesc{ID: id, Type: typ}
		}
		ff.fieldsByDocumentType[documentType] = fields
	}
	if err = buf.Error(); err != nil {
		return err
	}
	return ff.dict.ReadDesc(buf)
}
