package search

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring"

	"github.com/neganovalexey/search/codeerrors"
)

// StartToken can be used as page offset in paginated output
type StartToken struct {
	BlockID BlockID
	Offset  uint32
}

// String implements fmt.Stringer
func (tok StartToken) String() string {
	return fmt.Sprintf("%d.%d", tok.BlockID, tok.Offset)
}

// MarshalJSON implements json.Marshaler
func (tok StartToken) MarshalJSON() ([]byte, error) {
	return []byte(`"` + tok.String() + `"`), nil
}

// FromString builds StartToken from string representation
func (tok *StartToken) FromString(s string) error {
	_, err := fmt.Sscanf(s, "%d.%d", &tok.BlockID, &tok.Offset)
	return err
}

// UnmarshalJSON implements json.Unmarshaler
func (tok *StartToken) UnmarshalJSON(b []byte) (err error) {
	var s string
	if err = json.Unmarshal(b, &s); err != nil {
		return
	}
	return tok.FromString(s)
}

// FeatureDesc describes a feature in index.
// The Value should be
// time.Time if field is of type DateTime,
// string if field is of type ExactTerms
type FeatureDesc struct {
	DocumentType string      `json:"document_type"`
	FieldName    string      `json:"field_name"`
	Value        interface{} `json:"value"`
}

// Query represents search request.
type Query struct {
	Condition  QueryNode  `json:"condition"`
	StartToken StartToken `json:"start_token"`
	Limit      int

	blocks *roaring.Bitmap
}

// QueryNode is used to construct the boolean formula tree
type QueryNode struct {
	// directory nodes:
	Operation Operation   `json:"operation"` // AND, OR, NOR
	Children  []QueryNode `json:"children"`

	// child nodes:
	Subject FeatureDesc        `json:"subject"`
	rd      featureFieldReader // to be prepared
}

func (n *QueryNode) isDir() bool {
	return n.Operation != 0
}

func (n *QueryNode) isNegative() bool {
	return n.Operation == NOR
}

func (q *Query) prepare(index *Index) (err error) {
	if err = q.Condition.prepare(index.FeatureFields); err != nil {
		return err
	}
	q.blocks, err = q.Condition.getBlockBitmap(index.searcher, index.lastBlockID())
	if err != nil {
		return err
	}
	return nil
}

func (n *QueryNode) prepare(ff *FeatureFields) (err error) {
	if !n.isDir() {
		n.rd, err = ff.getReader(n.Subject)
		return err
	}

	newChildren := make([]QueryNode, 0, len(n.Children))
	for _, child := range n.Children {
		err = child.prepare(ff)
		if err == nil && (!child.isDir() || len(child.Children) > 0) {
			newChildren = append(newChildren, child)
		}
		if errors.Is(err, codeerrors.ErrNotFound) && (n.Operation == OR || n.Operation == NOR) {
			err = nil
		} else if err != nil {
			return err
		}
	}

	n.Children = newChildren
	if n.Operation == OR && len(newChildren) == 0 {
		return codeerrors.ErrNotFound
	}
	return nil
}

func (n *QueryNode) getBlockBitmap(s Searcher, lastBlockID BlockID) (*roaring.Bitmap, error) {
	if !n.isDir() {
		return n.rd.GetBlockBitmap(s)
	}

	var result *roaring.Bitmap

	if n.Operation == NOR {
		result = roaring.New()
		result.AddRange(0, uint64(lastBlockID+1))
		return result, nil
	}

	bitmaps := make([]*roaring.Bitmap, 0, len(n.Children))
	for i := range n.Children {
		bitmap, err := n.Children[i].getBlockBitmap(s, lastBlockID)
		if err != nil {
			return nil, err
		} else if bitmap == nil && n.Operation == AND {
			return nil, nil
		} else if bitmap == nil {
			continue
		}
		bitmaps = append(bitmaps, bitmap)
	}

	switch n.Operation {
	case AND:
		result = roaring.FastAnd(bitmaps...)
	case OR:
		result = roaring.FastOr(bitmaps...)
	}

	return result, nil
}

// clone queryNode WITHOUT state contents
func (n *QueryNode) clone() (clone QueryNode) {
	clone.Operation = n.Operation
	clone.Subject = n.Subject
	clone.Children = make([]QueryNode, 0, len(n.Children))
	for _, child := range n.Children {
		clone.Children = append(clone.Children, child.clone())
	}
	return
}

func not(b *roaring.Bitmap, blockSize uint32) *roaring.Bitmap {
	nb := roaring.New()
	nb.AddRange(0, uint64(blockSize))
	nb.AndNot(b)
	return nb
}

// Clone query WITHOUT state contents
func (q *Query) Clone() (clone Query) {
	clone.Limit = q.Limit
	clone.StartToken = q.StartToken
	clone.Condition = q.Condition.clone()
	return
}

func (q *Query) getBitmap(s Searcher, block BlockID) (*roaring.Bitmap, error) {
	result, err := q.Condition.getBitmap(s, block)
	if err != nil {
		return nil, err
	}
	if q.Condition.isNegative() {
		result = not(result, s.BitmapBlockSize())
	}
	return result, nil
}

func (n *QueryNode) getBitmap(s Searcher, block BlockID) (*roaring.Bitmap, error) {
	if !n.isDir() {
		return n.rd.GetBlock(s, block)
	}

	var result *roaring.Bitmap

	positive := make([]*roaring.Bitmap, 0, len(n.Children))
	negative := make([]*roaring.Bitmap, 0, len(n.Children))
	for i := range n.Children {
		bitmap, err := n.Children[i].getBitmap(s, block)
		if err != nil {
			return nil, err
		} else if bitmap == nil && n.Operation == AND {
			return nil, nil
		} else if bitmap == nil {
			continue
		}
		switch n.Children[i].Operation {
		case NOR:
			negative = append(negative, bitmap)
		default:
			positive = append(positive, bitmap)
		}
	}

	if len(positive) > 0 {
		switch n.Operation {
		case AND:
			result = roaring.FastAnd(positive...)
		case OR, NOR:
			result = roaring.FastOr(positive...)
		default:
			return nil, nil
		}
	} else {
		result = roaring.New()
		switch n.Operation {
		case AND:
			result.AddRange(0, uint64(s.BitmapBlockSize()))
		}
	}
	for _, neg := range negative {
		switch n.Operation {
		case AND:
			result.AndNot(neg)
		case OR, NOR:
			result.Or(not(neg, s.BitmapBlockSize()))
		}
	}

	return result, nil
}

func (q *Query) nextBlockFrom(from BlockID) (newBlockID BlockID, err error) {
	if q.blocks.Contains(from) {
		newBlockID = from
	} else {
		newBlockID, err = q.blocks.Select(uint32(q.blocks.Rank(from)))
	}
	return
}

// See https://github.com/RoaringBitmap/roaring/pull/150 for ManyIntIterable reasoning
func (index *Index) getBlockIterator(q *Query, from BlockID) (roaring.ManyIntIterable, BlockID, error) {
	newBlockID, err := q.nextBlockFrom(from)
	if err != nil {
		return nil, 0, nil // no more blocks
	}

	results, err := q.getBitmap(index.searcher, newBlockID)
	if err != nil {
		return nil, 0, err
	}

	return results.ManyIterator(), newBlockID, nil
}

func (index *Index) searchBlock(q *Query, start StartToken, limit int) (docIDs []uint64, newStart StartToken, err error) {
	batchSize := 100
	if limit < batchSize {
		batchSize = limit
	}
	buf := make([]uint32, batchSize)

	block, from, err := index.getBlockIterator(q, start.BlockID)
	if err != nil || block == nil {
		return nil, StartToken{}, err
	}
	var newOff uint32
	for len(docIDs) < limit {
		n := block.NextMany(buf)
		if n == 0 {
			break
		}
		for i := 0; i < n; i++ {
			v := buf[i]
			if v < start.Offset {
				continue
			}
			docIDs = append(docIDs, uint64(from)*uint64(index.searcher.BitmapBlockSize())+uint64(v))
			newOff = v + 1
		}
	}

	if len(docIDs) == limit && newOff < index.searcher.BitmapBlockSize() {
		newStart = StartToken{BlockID: from, Offset: newOff}
	} else {
		newStart = StartToken{BlockID: from + 1, Offset: 0}
	}

	return docIDs, newStart, nil
}
