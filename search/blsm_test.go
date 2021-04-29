package search_test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/search"
	"github.com/neganovalexey/search/testutil"
	aio "github.com/neganovalexey/search/io"
)

type findResult struct {
	feature search.FeatureID
	field   search.FieldID
	offs    uint64
}

func createTestBlsm(io io.LsmIo) *search.BitLsm {
	return search.NewBitLsm(search.BitLsmCfg{
		Name:         "test-lsm",
		ID:           1,
		Io:           io,
		Cache:        cache.New(1024 * 1024),
		Log:          logrus.New(),
		BitsPerBlock: 2,
	})
}

func TestBitLsmBasic(t *testing.T) {
	io, err := aio.New(testutil.GetTestIoConfig(logrus.New()))
	require.NoError(t, err)

	lsmIo := io.CreateLsmIo("", "test-index", cache.New(1024*1024*2), new(aio.ObjectGcTest))

	blsm := createTestBlsm(lsmIo)

	require.NoError(t, blsm.Insert(0, 0, 1))
	require.NoError(t, blsm.Flush())
	require.NoError(t, blsm.Insert(0, 0, 10))
	require.NoError(t, blsm.Insert(0, 0, 11))
	require.NoError(t, blsm.Insert(0, 1, 12))
	require.NoError(t, blsm.Insert(1, 0, 100))
	require.NoError(t, blsm.Flush())
	require.NoError(t, blsm.Insert(2, 0, 14))

	check := func(blsm *search.BitLsm, op int, feature search.FeatureID, field search.FieldID, from search.BlockID, expected []findResult) {
		it := blsm.Find(op, feature, field, from)
		var got []findResult
		for ; !it.Empty(); it.Next() {
			feature, field, block, bitmap := it.Result()
			bitmap.Iterate(func(x uint32) bool {
				got = append(got, findResult{
					feature: feature,
					field:   field,
					offs:    uint64(block)*uint64(blsm.BitsPerBlock) + uint64(x),
				})
				return true
			})
			if op == lsm.EQ {
				break
			}
		}
		require.NoError(t, it.Error())
		require.Equal(t, expected, got)
	}

	checks := func(blsm *search.BitLsm) {
		check(blsm, lsm.GE, 1, 0, 0, []findResult{
			{1, 0, 100}})
		check(blsm, lsm.LE, 1, 0, 50, []findResult{
			{1, 0, 100},
		})
		check(blsm, lsm.LT, 1, 0, 51, []findResult{
			{1, 0, 100},
		})
		check(blsm, lsm.LT, 1, 0, 50, nil)
		check(blsm, lsm.EQ, 0, 0, 5, []findResult{
			{0, 0, 10},
			{0, 0, 11},
		})
		check(blsm, lsm.GE, 0, 1, 6, []findResult{
			{0, 1, 12},
		})
		check(blsm, lsm.EQ, 0, 1, 6, []findResult{
			{0, 1, 12},
		})
		check(blsm, lsm.EQ, 100500, 0, 0, nil)
		check(blsm, lsm.GE, 0, 2, 0, nil)
		check(blsm, lsm.GE, 0, 0, 6, nil)
		check(blsm, lsm.GT, 0, 0, 5, nil)

		// all items checks
		check(blsm, lsm.GE, 0, 0, 0, []findResult{
			{0, 0, 1},
			{0, 0, 10},
			{0, 0, 11}})
		check(blsm, lsm.GE, 0, 1, 0, []findResult{
			{0, 1, 12}})
		check(blsm, lsm.GE, 0, 1, 0, []findResult{
			{0, 1, 12}})
		check(blsm, lsm.GE, 1, 0, 0, []findResult{
			{1, 0, 100}})
		check(blsm, lsm.GE, 2, 0, 0, []findResult{
			{2, 0, 14}})

		// note reverse block order
		check(blsm, lsm.LE, 0, 0, 100, []findResult{
			{0, 0, 10},
			{0, 0, 11},
			{0, 0, 1}})
		check(blsm, lsm.LE, 0, 1, 100, []findResult{
			{0, 1, 12}})
		check(blsm, lsm.LE, 0, 1, 100, []findResult{
			{0, 1, 12}})
		check(blsm, lsm.LE, 1, 0, 100, []findResult{
			{1, 0, 100}})
		check(blsm, lsm.LE, 2, 0, 100, []findResult{
			{2, 0, 14}})
	}

	checks(blsm)
	require.NoError(t, blsm.Flush())
	sbuf := lsm.NewSerializeBuf(10)
	blsm.WriteDesc(sbuf)
	dbuf := lsm.NewDeserializeBuf(sbuf.Bytes())

	blsm2 := createTestBlsm(lsmIo)
	require.NoError(t, blsm2.ReadDesc(dbuf))
	checks(blsm2)
}
