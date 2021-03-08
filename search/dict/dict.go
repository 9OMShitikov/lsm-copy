package dict

import (
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/lsm"
)

type (
	// FeatureID is a data type for feature ID
	FeatureID = uint64
	// FieldID is a data type for field ID
	FieldID = uint16
	// Term is search atomic item
	Term = string
)

// Dictionary maps Term -> FeatureID
type Dictionary interface {
	Get(field FieldID, term Term) (FeatureID, error)
	GetOrInsert(field FieldID, term Term) (FeatureID, error)
	ReadDesc(buf *lsm.DeserializeBuf) error
	WriteDesc(buf *lsm.SerializeBuf)
	// WaitAllBackgroundInsertionRoutines waits until all background insertion procedures not complete.
	// Should be called before WriteDesc
	WaitAllBackgroundInsertionRoutines()
}

// Cfg is Dictionary config
type Cfg struct {
	Name string
	Log  *logrus.Logger
	IO   io.LsmIo
}

// New initializes Dictionary from given config
func New(cfg Cfg) Dictionary {
	return newLsmDict(cfg)
}
