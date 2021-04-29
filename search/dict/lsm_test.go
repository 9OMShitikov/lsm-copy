package dict

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/lsm"
	"github.com/neganovalexey/search/testutil"
	aio "github.com/neganovalexey/search/io"
)

func TestLsmDictionary(t *testing.T) {
	io, err := aio.New(testutil.GetTestIoConfig(logrus.New()))
	require.NoError(t, err)
	defer func() {
		// TODO: cleanup files
	}()

	cfg := Cfg{
		Name: "dicc",
		IO:   io.CreateLsmIo("", "test-dict", cache.New(1024*1024*2), new(aio.ObjectGcTest)),
	}
	insert := func(dict *lsmDict, te Term, eid FeatureID) {
		id, err := dict.GetOrInsert(1, te)
		require.NoError(t, err)
		require.Equal(t, eid, id)
	}

	has := func(dict *lsmDict, te Term, eid FeatureID) {
		id, err := dict.Get(1, te)
		require.NoError(t, err)
		require.Equal(t, eid, id)
	}
	notFound := func(dict *lsmDict, te Term) {
		_, err := dict.Get(1, te)
		require.True(t, errors.Is(err, errTermNotFound))
	}
	dict := newLsmDict(cfg)
	insert(dict, "a", 1)
	insert(dict, "ab", 2)
	insert(dict, "ab", 2)
	insert(dict, "c", 3)

	has(dict, "a", 1)
	has(dict, "ab", 2)
	has(dict, "c", 3)
	notFound(dict, "d")

	buf := lsm.NewSerializeBuf(10)
	dict.WriteDesc(buf)
	dbuf := lsm.NewDeserializeBuf(buf.Bytes())

	dict2 := newLsmDict(cfg)
	require.NoError(t, dict2.ReadDesc(dbuf))
	has(dict2, "a", 1)
	has(dict2, "ab", 2)
	has(dict2, "c", 3)
	notFound(dict2, "d")
	insert(dict2, "d", 4)
	has(dict2, "d", 4)
}
