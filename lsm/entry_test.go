package lsm

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"unsafe"

	"github.com/neganovalexey/search/cache"
	aio "github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/rbtree"
)

type testEntryKey struct {
	str string
}

type testEntry struct {
	BaseEntry
	data uint32
	str  string
}

func testEntryCmp(a, b interface{}) int {
	s1 := a.(*testEntry).str
	s2 := b.(*testEntry).str
	return keysCmp(s1, s2)
}

func testEntryKeyCmp(a, b interface{}) int {
	s1 := a.(*testEntry).str
	s2 := b.(*testEntryKey).str

	return keysCmp(s1, s2)
}

func testEntry2KeysCmp(a, b interface{}) int {
	s1 := a.(*testEntryKey).str
	s2 := b.(*testEntryKey).str

	return keysCmp(s1, s2)
}

func keysCmp(s1, s2 string) int {
	min := len(s2)
	if len(s1) < len(s2) {
		min = len(s1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(s1[i]) - int(s2[i])
	}
	if diff == 0 {
		diff = len(s1) - len(s2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

func (e *testEntry) Size() int {
	return int(unsafe.Sizeof(*e)) + len(e.str)
}

func (e *testEntry) Encode(buf *SerializeBuf, prev Entry) {
	buf.EncodeStr(e.str)
	buf.EncodeUint32(e.data)
}

func (e *testEntry) Decode(buf *DeserializeBuf, prev Entry) error {
	e.str = buf.DecodeStr()
	e.data = buf.DecodeUint32()

	return buf.Error()
}

func (e *testEntry) String() string {
	return fmt.Sprintf("{str: %v, data: %v, del: %v}", e.str, e.data, e.lsmDelFlag)
}

func (e *testEntry) GetKey() EntryKey {
	return &testEntryKey{str: e.str}
}

func testEntryCreate() Entry {
	return new(testEntry)
}

func (e *testEntryKey) Encode(buf *SerializeBuf) {
	buf.EncodeStr(e.str)
}

func (e *testEntryKey) Decode(buf *DeserializeBuf) error {
	e.str = buf.DecodeStr()
	return buf.Error()
}

func (e *testEntryKey) String() string {
	return fmt.Sprintf("{str: %v}", e.str)
}

func (e *testEntryKey) Size() int {
	return int(unsafe.Sizeof(*e)) + len(e.str)
}

func testEntryCreateKey() EntryKey {
	return new(testEntryKey)
}

// ---------------------------------------------------------------------------

type dummyLsmIO struct{}

func (*dummyLsmIO) Ctx() context.Context { return context.Background() }

func (*dummyLsmIO) ReadPages(ctreeID aio.CtreeID, off int64, sz int64) (data []byte, err error) {
	return
}
func (*dummyLsmIO) NewMergeWriter(dstCtreeID aio.CtreeID) aio.MergeWriter { return nil }
func (*dummyLsmIO) FreeBlocks(blocks []aio.CTreeNodeBlock) (err error)    { return }

func TestEntryEncodeDecode(t *testing.T) {
	lsm := New(
		Config{
			Io:                &dummyLsmIO{},
			Comparator:        testEntryCmp,
			ComparatorWithKey: testEntryKeyCmp,
			Comparator2Keys:   testEntry2KeysCmp,
			CreateEntry:       testEntryCreate,
			CreateEntryKey:    testEntryCreateKey,
			Cache:             cache.New(1024 * 1024),
		})
	tree := rbtree.New(testEntryCmp, testEntryKeyCmp)

	for i := 10; i < 100; i++ {
		e := &testEntry{str: strconv.Itoa(i), data: uint32(i)}
		if i%2 == 0 {
			e.SetLsmDeleted(true)
		}
		tree.Insert(e)
	}
	fakeCtree := &ctree{lsm: lsm} // hack: needed for encode/decodeEntries

	hdr := fakeCtree.encodeEntries(tree.First(), 32768)
	testCfg.Log.Infof("buf=%#v delmask=%#v", hdr.items, hdr.delmask)

	// decode entries and compare
	tree2, _, _, err := fakeCtree.decodeEntries(hdr)
	fatalOnErr(err)
	if tree2.Count() != tree.Count() {
		t.Errorf("restored tree count is wrong")
	}
	testCfg.Log.Infof("%#v", tree2)
	tree.ForEach(func(i interface{}) bool {
		node2 := tree2.FindEq(i.(*testEntry).GetKey())
		if node2.Empty() {
			t.Errorf("testEntry2 not found")
			return false
		}

		e := i.(*testEntry)
		e2 := node2.Item().(*testEntry)
		if e.data != e2.data || e.str != e2.str || e.LsmDeleted() != e2.LsmDeleted() {
			t.Errorf("testEntry2 doesn't match testEntry1")
			return false
		}
		return true
	})

	/* check corrupted buf */
	hdr.items = append(hdr.items, byte(20)) // append just in case whole data was aligned and, otherwise 0 byte from padding will be used...
	hdr.len++
	tree2, _, _, err = fakeCtree.decodeEntries(hdr)
	testCfg.Log.Infof("corrupted tree decoding: err=%#v", err)
	if err == nil {
		t.Errorf("Corrupted buffer decoded w/o error")
	}
}
