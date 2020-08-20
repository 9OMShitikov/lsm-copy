package main

import (
	"sync/atomic"
	"unsafe"

	"fmt"
	"sync"
	"math/rand"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
	"github.com/neganovalexey/search/lsm"
	aio "github.com/neganovalexey/search/io"
)

type testEntryKey struct {
	str string
}

type BaseEntry struct {
	lsmDelFlag bool // entry is marked as deleted in LSM tree
}

type testEntry struct {
	BaseEntry
	data uint32
	str  string
}

type ctreeStats struct {
	LeafsSize int64 `json:"leafs_size"`
	DirsSize  int64 `json:"dirs_size"`
	Count     int64 `json:"count"`
	Deleted   int64 `json:"deleted"`
}

type ctree struct {
	Idx int `json:"idx"` // Ctree index in LSM tree

	merging      bool   // used in merge at the moment
	mergeDiskGen uint64 // next DiskGeneration value assigned after merge is finished

	Generation     uint64 `json:"-"`   // generation of ctree in mem, changes every time when ctree is modified
	DiskGeneration uint64 `json:"gen"` // generation of ctree, changes every time when ctree is modified

	RootOffs int64        `json:"root_offs"`
	RootSize int64        `json:"root_size"`
	root     atomic.Value // ptr to ctreeNode
	Stats    ctreeStats   `json:"stats"`

	// bloom filter for this ctree (nil means no bloom filter is used or not read yet).
	// Used only for disk trees. This filter is updated only during merge
	bloom                   atomic.Value
	bloomLoadAfterMissCount int64 // if bloom stores with ctree (not in lsm desc) => bloom will read after this counter become 0
	BloomOffs               int64 `json:"bloom_offs"`
	BloomSize               int64 `json:"bloom_size"`
	bloomReadMtx            sync.Mutex

	lsm *lsm.Lsm
}

type lsmGenerationSupplier interface {
	nextLsmGen() uint64
	nextCTreeDiskGen(idx int) uint64
}

type testConfig struct {
	name    string
	io      aio.Io
	logFile string

	Log *logrus.Logger
}

var testCfg testConfig

func createTestLsmIo() (io aio.LsmIo) {
	testCfg.name = "test-lsm"

	testCashe := cache.New(1024*1024*2)
	testGb := new(aio.ObjectGcTest)

	return testCfg.io.CreateLsmIo("", testCfg.name, testCashe, testGb)
}


func createTestLsm() (lsm_example *lsm.Lsm) {
	var cacheSize int64 = 1024 * 1024

	io := createTestLsmIo()

	enableBloom := false
	if rand.Intn(2) > 0 {
		enableBloom = true
		testCfg.Log.Infof("Bloom filter enabled")
	}

	cfg := lsm.Config{
		Name:              testCfg.name,
		ID:                1,
		Io:                io,
		Comparator:        testEntryCmp,
		ComparatorWithKey: testEntryKeyCmp,
		Comparator2Keys:   testEntry2KeysCmp,
		CreateEntry:       testEntryCreate,
		CreateEntryKey:    testEntryCreateKey,
		Cache:             cache.New(cacheSize),
		Log:               testCfg.Log,
		EnableBloomFilter: enableBloom,
	}
	lsm_example = lsm.New(cfg)
	return lsm_example
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

func (e *testEntry) Encode(buf *lsm.SerializeBuf, prev lsm.Entry) {
	buf.EncodeStr(e.str)
	buf.EncodeUint32(e.data)
}

func (e *testEntry) Decode(buf *lsm.DeserializeBuf, prev lsm.Entry) error {
	e.str = buf.DecodeStr()
	e.data = buf.DecodeUint32()

	return buf.Error()
}

func (e *testEntry) String() string {
	return fmt.Sprintf("{str: %v, data: %v, del: %v}", e.str, e.data, e.BaseEntry)
}

func (e *testEntry) GetKey() lsm.EntryKey {
	return &testEntryKey{str: e.str}
}
/*
func testEntryCreate() lsm.Entry {
	return new(testEntry)
}
*/
func (e *testEntryKey) Encode(buf *lsm.SerializeBuf) {
	buf.EncodeStr(e.str)
}

func (e *testEntryKey) Decode(buf *lsm.DeserializeBuf) error {
	e.str = buf.DecodeStr()
	return buf.Error()
}

func (e *testEntryKey) String() string {
	return fmt.Sprintf("{str: %v}", e.str)
}

func (e *testEntryKey) Size() int {
	return int(unsafe.Sizeof(*e)) + len(e.str)
}

func testEntryCreateKey() lsm.EntryKey {
	return new(testEntryKey)
}

func testEntryCreate() lsm.Entry {
	return new(testEntry)
}

func (e *testEntry) LsmDeleted() bool {
	return e.BaseEntry.lsmDelFlag
}

func (e *testEntry) SetLsmDeleted(d bool) {
	e.BaseEntry.lsmDelFlag = d
}
