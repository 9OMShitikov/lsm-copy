package search_test

import (
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/search"
	"github.com/neganovalexey/search/search/dict"
	"github.com/neganovalexey/search/testutil"
	aio "github.com/neganovalexey/search/io"
)

//nolint:unparam
func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

const docTypeMail = "mail"

func testIndexSearch(t *testing.T, idx *search.Index) {
	dateField, bodyField := "date", "body"
	err := idx.FeatureFields.RegisterFieldIfNeeded(docTypeMail, dateField, search.FieldTypeDateTime)
	require.NoError(t, err)
	err = idx.FeatureFields.RegisterFieldIfNeeded(docTypeMail, bodyField, search.FieldTypeExactTerms)
	require.NoError(t, err)

	err = idx.AddDocument(search.DocID{InodeID: 1}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"b"}},
		{FieldName: dateField, Value: date(2020, time.August, 14)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 2}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a", "b", "c"}},
		{FieldName: dateField, Value: date(2020, time.August, 15)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 3}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a"}},
		{FieldName: dateField, Value: date(2020, time.August, 16)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 4}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a", "b", "c", "d"}},
		{FieldName: dateField, Value: date(2020, time.August, 17)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 5}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a", "b", "c"}},
		{FieldName: dateField, Value: date(2020, time.January, 14)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 6}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"e"}},
		{FieldName: dateField, Value: date(2020, time.January, 15)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 7}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a", "b", "f"}},
		{FieldName: dateField, Value: date(2020, time.January, 16)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 8}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"a", "b"}},
		{FieldName: dateField, Value: date(2020, time.January, 17)},
	})
	require.NoError(t, err)
	err = idx.AddDocument(search.DocID{InodeID: 100}, docTypeMail, []search.FieldData{
		{FieldName: bodyField, Value: []dict.Term{"c"}},
		{FieldName: dateField, Value: date(2020, time.August, 25)},
	})
	require.NoError(t, err)

	t.Run("simple", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 2}, {InodeID: 4}, {InodeID: 5}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and", func(t *testing.T) {
		docIDs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 2}, {InodeID: 4}, {InodeID: 5}, {InodeID: 7}, {InodeID: 8}}, docIDs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("or", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.OR,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "d"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "e"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "z"}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 4}, {InodeID: 6}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-or", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "d"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "e"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("not", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.NOR,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}, {InodeID: 6}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-not", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("or-not", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"}},
					}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-or-not", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "f"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "j"}},
					}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 7}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("simple-not-in-dict", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "z"},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Empty(t, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("or-not-in-dict", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.OR,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "z"}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Empty(t, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
					From: date(2020, time.August, 1),
					To:   date(2020, time.September, 1),
				}},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}, {InodeID: 2}, {InodeID: 3}, {InodeID: 4}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("simple-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 2}, {InodeID: 3}, {InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 2}, {InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("or-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "d"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "e"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-or-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "d"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "e"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("not-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-not-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("or-not-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"}},
					}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 1}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("and-or-not-date", func(t *testing.T) {
		docs, start, err := idx.Search(&search.Query{
			Condition: search.QueryNode{
				Operation: search.AND,
				Children: []search.QueryNode{
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "a"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "b"}},
					{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: dateField, Value: search.DateRange{
						From: date(2020, time.August, 1),
						To:   date(2020, time.September, 1),
					}}},
					{Operation: search.OR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "f"}},
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "j"}},
					}},
					{Operation: search.NOR, Children: []search.QueryNode{
						{Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"}},
					}},
				},
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Empty(t, docs)
		require.Equal(t, search.StartToken{}, start)
	})
	t.Run("simple-limit", func(t *testing.T) {
		q := search.Query{
			Condition: search.QueryNode{
				Subject: search.FeatureDesc{DocumentType: docTypeMail, FieldName: bodyField, Value: "c"},
			},
			Limit: 2,
		}
		docs, start, err := idx.Search(&q)
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 2}, {InodeID: 4}}, docs)
		require.Equal(t, search.StartToken{BlockID: 1, Offset: 1}, start)

		q.StartToken = start
		docs, start, err = idx.Search(&q)
		require.NoError(t, err)
		require.Equal(t, []search.DocID{{InodeID: 5}, {InodeID: 100}}, docs)
		require.Equal(t, search.StartToken{BlockID: 2, Offset: 2}, start)

		q.StartToken = start
		docs, start, err = idx.Search(&q)
		require.NoError(t, err)
		require.Empty(t, docs)
		require.Equal(t, search.StartToken{}, start)
	})
}

func TestIndex_SearchMocked(t *testing.T) {
	searcher := testSearcher{bitmaps: make(map[testSearcherKey]*roaring.Bitmap)}
	dictionary := testDictionary{make(map[dict.Term]search.FeatureID)}
	mapping := testArchiveMapping{doc2inodePit: make(map[uint64]search.DocID)}
	idx, err := search.NewIndexFrom(&searcher, &dictionary, &mapping)
	require.NoError(t, err)
	testIndexSearch(t, idx)
}

func TestIndex_SearchLsm(t *testing.T) {
	io, err := aio.New(testutil.GetTestIoConfig(logrus.New()))
	require.NoError(t, err)

	idx, err := search.NewIndex(&search.IndexConfig{
		Name:             "test-index",
		Io:               io,
		GC:               new(aio.ObjectGcTest),
		GetPrefetchCache: func() *cache.Cache { return cache.New(2 * 1024 * 1024) },
		Log:              logrus.New(),
		BitsPerBlock:     4,
	})
	require.NoError(t, err)

	testIndexSearch(t, idx)
}
