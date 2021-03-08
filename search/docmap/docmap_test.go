package docmap

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/testutil"
	aio "github.com/neganovalexey/search/io"
)

func TestLsmDocMap(t *testing.T) {
	io, err := aio.New(testutil.GetTestIoConfig(logrus.New()))
	require.NoError(t, err)
	defer func() {
		// TODO: cleanup files
	}()

	cfg := Cfg{
		Name: "amapp",
		IO:   io.CreateLsmIo("", "test-amap", cache.New(1024*1024*2), new(aio.ObjectGcTest)),
	}
	insert := func(amap DocMap, docID, inodeID, pitID uint64) {
		err := amap.Insert(docID, inodeID, pitID)
		require.NoError(t, err)
	}

	has := func(amap DocMap, docID, inodeID, pitID uint64) {
		eInodeID, ePitID, err := amap.LookupInodeVersion(docID)
		require.NoError(t, err)
		require.Equal(t, inodeID, eInodeID)
		require.Equal(t, pitID, ePitID)
	}
	notFound := func(amap DocMap, docID uint64) {
		_, _, err := amap.LookupInodeVersion(docID)
		require.True(t, errors.Is(err, ErrDocNotFound))
	}
	amap := NewDocMap(cfg)
	insert(amap, 1, 1, 1)
	insert(amap, 2, 2, 1)
	insert(amap, 3, 10, 3)

	has(amap, 1, 1, 1)
	has(amap, 2, 2, 1)
	has(amap, 3, 10, 3)
	notFound(amap, 4)
	notFound(amap, 0)
	notFound(amap, 100)

	buf := lsm.NewSerializeBuf(10)
	amap.WriteDesc(buf)
	dbuf := lsm.NewDeserializeBuf(buf.Bytes())

	dict2 := NewDocMap(cfg)
	require.NoError(t, dict2.ReadDesc(dbuf))
	has(amap, 1, 1, 1)
	has(amap, 2, 2, 1)
	has(amap, 3, 10, 3)
	notFound(amap, 4)
	insert(amap, 4, 100, 4)
	has(amap, 4, 100, 4)
}
