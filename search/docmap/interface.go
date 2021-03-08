package docmap

import (
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/codeerrors"
	"github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/lsm"
)

// DocMap maps docIDs into InodeVersion (can described by pair (InodeID, PitID))
type DocMap interface {
	Insert(docID, inodeID, pitID uint64) (err error)
	LookupInodeVersion(docID uint64) (inodeID, pitID uint64, err error)
	WriteDesc(buf *lsm.SerializeBuf)              // serialize descriptor (not only for lsm implementations)
	ReadDesc(buf *lsm.DeserializeBuf) (err error) // deserialize descriptor (not only for lsm implementations)
	// WaitAllBackgroundInsertionRoutines waits until all background insertion procedures not complete.
	// Should be called before WriteDesc
	WaitAllBackgroundInsertionRoutines()
}

// Cfg is DocMap config
type Cfg struct {
	Name string
	Log  *logrus.Logger
	IO   io.LsmIo
}

var (
	// ErrDocNotFound returned on lookup if no doc found
	ErrDocNotFound = codeerrors.ErrNotFound.WithMessage("term not found in dictionary")
)

// NewDocMap returns DOcMap by given config
func NewDocMap(cfg Cfg) DocMap {
	return newlsmDocMap(cfg)
}
