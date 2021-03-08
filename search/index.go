package search

import (
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/codeerrors"
	"github.com/neganovalexey/search/cache"
	aio "github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/search/dict"
	"github.com/neganovalexey/search/search/docmap"
)

// Index allows to search over documents
type Index struct {
	searcher      Searcher
	doc2InodePit  docmap.DocMap
	FeatureFields *FeatureFields
	lastDocID     uint64
}

// NewIndexFrom instantiates Index from existing Searcher & Dictionary.
// To be used primarily in tests
func NewIndexFrom(searcher Searcher, dict dict.Dictionary, doc2InodePit docmap.DocMap) (*Index, error) {
	ff, err := NewFeatureFields(dict)
	if err != nil {
		return nil, err
	}
	return &Index{
		searcher:      searcher,
		doc2InodePit:  doc2InodePit,
		FeatureFields: ff,
	}, nil
}

// IndexConfig contains params for index creation
type IndexConfig struct {
	Name             string
	LSMFolder        string
	Io               aio.Io
	GC               aio.ObjectGc
	GetPrefetchCache func() *cache.Cache
	BitsPerBlock     uint32
	Log              *logrus.Logger
}

// NewIndex instantiates Index
func NewIndex(cfg *IndexConfig) (*Index, error) {
	searcherName := cfg.Name + "-searcher"
	searcher := NewBitLsm(BitLsmCfg{
		Name:         searcherName,
		ID:           'b'<<24 + 'l'<<16 + 's'<<8 + 'm',
		Io:           cfg.Io.CreateLsmIo(cfg.LSMFolder, searcherName, cfg.GetPrefetchCache(), cfg.GC),
		Cache:        cache.New(1024 * 1024),
		Log:          cfg.Log,
		BitsPerBlock: cfg.BitsPerBlock,
	})

	dictName := cfg.Name + "-dict"
	dictionary := dict.New(dict.Cfg{
		Name: dictName,
		IO:   cfg.Io.CreateLsmIo(cfg.LSMFolder, dictName, cfg.GetPrefetchCache(), cfg.GC),
		Log:  cfg.Log,
	})

	docMapName := cfg.Name + "-docmap"
	doc2InodePit := docmap.NewDocMap(docmap.Cfg{
		Name: docMapName,
		IO:   cfg.Io.CreateLsmIo(cfg.LSMFolder, docMapName, cfg.GetPrefetchCache(), cfg.GC),
		Log:  cfg.Log,
	})

	return NewIndexFrom(searcher, dictionary, doc2InodePit)
}

// FieldData describes a field to be indexed
// Value should be of the type
//   time.Time for field of type DateTime,
//   []Term  for field of type ExactTerms
type FieldData struct {
	FieldName string
	Value     interface{}
}

// DocID identifies a document unambiguously
type DocID struct {
	InodeID uint64
	PitID   uint64
}

// AddDocument indexes a document with given ID and fields
func (index *Index) AddDocument(doc DocID, docType string, fields []FieldData) error {
	docID := atomic.AddUint64(&index.lastDocID, 1)

	if err := index.doc2InodePit.Insert(docID, doc.InodeID, doc.PitID); err != nil {
		return err
	}

	for _, field := range fields {
		writer, err := index.FeatureFields.getWriter(docType, field.FieldName)
		if err != nil {
			return errors.Wrap(err, "AddDocumentToIndex()")
		}
		if err = writer.Insert(index.searcher, docID, field.Value); err != nil {
			return errors.Wrap(err, "AddDocumentToIndex()")
		}
	}

	return nil
}

// Search obtains documents from the index.
// Query object is mutating during call, because is contains the state of query execution.
// In order to reuse Query in benchmarks, one should use Query.Clone() method before Index.Search() call
func (index *Index) Search(q *Query) (docs []DocID, start StartToken, err error) {
	err = q.prepare(index)
	if errors.Is(err, codeerrors.ErrNotFound) || q.blocks == nil || q.blocks.IsEmpty() {
		return nil, StartToken{}, nil
	} else if err != nil {
		return
	}

	for start = q.StartToken; len(docs) < q.Limit; {
		var blockResult []uint64
		blockResult, start, err = index.searchBlock(q, start, q.Limit-len(docs))
		if err != nil || (start.BlockID == 0 && start.Offset == 0) {
			return docs, StartToken{}, err
		}
		for _, id := range blockResult {
			// TODO: use workgroup & lookup in parallel
			inode, pit, err := index.doc2InodePit.LookupInodeVersion(id)
			if errors.Is(err, codeerrors.ErrNotFound) {
				// i.e. pure NOT query may return non-existent IDs
				continue
			} else if err != nil {
				return nil, StartToken{}, err
			}
			docs = append(docs, DocID{InodeID: inode, PitID: pit})
		}
	}

	return docs, start, nil
}

// lastBlockID returns the current max block ID in the index
func (index *Index) lastBlockID() BlockID {
	return BlockID(index.lastDocID / uint64(index.searcher.BitmapBlockSize()))
}

const (
	indexMagic   = ('S' << 24) + ('I' << 16) + ('D' << 8) + 'X'
	indexVersion = 1
)

// WriteDesc serializes Index
func (index *Index) WriteDesc(buf *lsm.SerializeBuf) {
	buf.EncodeFixedUint32(indexMagic)
	buf.EncodeFixedUint32(indexVersion)
	buf.EncodeUint64(index.lastDocID)
	index.FeatureFields.WriteDesc(buf)
	index.searcher.WriteDesc(buf)
	index.doc2InodePit.WriteDesc(buf)
}

// ReadDesc deserializes Index
func (index *Index) ReadDesc(buf *lsm.DeserializeBuf) (err error) {
	magic := buf.DecodeFixedUint32()
	if magic != indexMagic {
		return lsm.FirstErr(buf.Error(), fmt.Errorf("invalid index magic: %v", magic))
	}
	version := buf.DecodeFixedUint32()
	if version > indexVersion {
		return lsm.FirstErr(buf.Error(), fmt.Errorf("index version greater than supported: %d > %d", version, indexVersion))
	}
	index.lastDocID = buf.DecodeUint64()
	if err = buf.Error(); err != nil {
		return err
	}
	if err = index.FeatureFields.ReadDesc(buf); err != nil {
		return err
	}
	if err = index.searcher.ReadDesc(buf); err != nil {
		return err
	}
	if err = index.doc2InodePit.ReadDesc(buf); err != nil {
		return err
	}
	return nil
}

// WaitAllBackgroundInsertionRoutines waits until all background insertion procedures not complete.
// Should be called before WriteDesc
func (index *Index) WaitAllBackgroundInsertionRoutines() {
	index.searcher.WaitAllBackgroundInsertionRoutines()
	index.FeatureFields.dict.WaitAllBackgroundInsertionRoutines()
	index.doc2InodePit.WaitAllBackgroundInsertionRoutines()
}
