package lsm

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/google/gops/agent"
	"github.com/neganovalexey/search/cache"
	aio "github.com/neganovalexey/search/io"
	"github.com/neganovalexey/search/rbtree"
)

type testConfig struct {
	name    string
	io      aio.Io
	logFile string

	Log *logrus.Logger
}

var testCfg testConfig

func initLogging() {
	flag.StringVar(&testCfg.logFile, "log", "", "log file")
	flag.Parse()
	formatter := new(logrus.TextFormatter)
	out := os.Stderr
	testCfg.logFile = "my-lsm-log.log"
	if testCfg.logFile != "" {
		out, _ = os.OpenFile(testCfg.logFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	} else {
		formatter = &logrus.TextFormatter{ForceColors: true}
	}

	ll := logrus.New()
	ll.Out = out
	ll.Formatter = formatter

	testCfg.Log = ll
}

// GetTestIoConfig constructs IO config with specified logger
func GetTestIoConfig(log *logrus.Logger) aio.Config {
	return aio.Config{
		Root: "test-folder-" + randomStrKey(int(time.Now().UnixNano()), 16),
		Log:  log,
		Ctx:  context.Background(),
	}
}

func TestMain(m *testing.M) {
	initLogging()

	_ = agent.Listen(agent.Options{})

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	testCfg.Log.Infof("SEED = %d", seed)

	TestModeOn()

	os.Exit(m.Run())
}

// randomStrKey returns stable random key for given n of length sz
// nolint: unparam
func randomStrKey(n, sz int) (res string) {
	for len(res) < sz {
		buf := [8]byte{}
		binary.BigEndian.PutUint64(buf[:], uint64(n))
		h := sha512.Sum512(buf[:])
		res = res + base64.StdEncoding.EncodeToString(h[:])
	}
	return res[:sz]
}

func beforeTest() {
	var err error

	iocfg := GetTestIoConfig(testCfg.Log)
	testCfg.io, err = aio.New(iocfg)
	fatalOnErr(err)

	fmt.Println("Test io root : ", iocfg.Root)
}

func afterTest() {
	// TODO: cleanup files
}

func createTestLsmIo() (io aio.LsmIo) {
	testCfg.name = "test-lsm"
	return testCfg.io.CreateLsmIo("", testCfg.name, cache.New(1024*1024*2), new(aio.ObjectGcTest))
}

func createTestLsmWithBloom(enableBloom bool) (lsm *Lsm) {
	var cacheSize int64 = 1024 * 1024

	io := createTestLsmIo()

	cfg := Config{
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
	lsm = New(cfg)
	return
}


func createTestLsm() (lsm *Lsm) {
	enableBloom := false
	if rand.Intn(2) > 0 {
		enableBloom = true
		testCfg.Log.Infof("Bloom filter enabled")
	}

	lsm = createTestLsmWithBloom(enableBloom)
	return
}

func createTestRb() *rbtree.Tree {
	return rbtree.New(testEntryCmp, testEntryKeyCmp)
}

func checkLsm(lsm *Lsm, rb *rbtree.Tree) (err error) {
	return checkIteratorsEqual(lsm.Find(GE, nil), rb.First(), false)
}

func checkIteratorsEqual(lsmIt *Iter, rbIt rbtree.OrderedIter, rbGoPrev bool) (err error) {
	for !lsmIt.Empty() {
		if !reflect.DeepEqual(lsmIt.Result, rbIt.Item()) {
			err = fmt.Errorf("lsm mismatch %v vs. rbtree %v", lsmIt.Result, rbIt.Item())

			lsmIt.Next()
			rbIt.Next()
			if rbIt.Empty() || lsmIt.Empty() {
				return err
			}
			lsmIt.lsm.debug2("cmp next %v %v\n", lsmIt.Result, rbIt.Item())
			lsmIt.Next()
			rbIt.Next()
			if rbIt.Empty() || lsmIt.Empty() {
				return err
			}
			lsmIt.lsm.debug2("cmp next %v %v\n", lsmIt.Result, rbIt.Item())
			lsmIt.Next()
			rbIt.Next()
			if rbIt.Empty() || lsmIt.Empty() {
				return err
			}
			lsmIt.lsm.debug2("cmp next %v %v\n", lsmIt.Result, rbIt.Item())

			return err
		}
		lsmIt.Next()

		if rbGoPrev {
			rbIt.Prev()
		} else {
			rbIt.Next()
		}
	}

	if err = lsmIt.Error(); err != nil {
		return err
	}
	if lsmIt.Empty() != rbIt.Empty() {
		return fmt.Errorf("one of the trees reach EOF earlier then another")
	}
	return
}

// nolint: unused, deadcode
func printIterator(it *Iter) {
	fmt.Println("ITERATOR VALUES: ")
	for !it.Empty() {
		fmt.Println(it.Result)
		it.Next()
	}
	fatalOnErr(it.Error())
}

// nolint: unused, deadcode
func printRbIterator(it rbtree.OrderedIter, goPrev bool) {
	fmt.Println("RB ITERATOR VALUES: ")
	for !it.Empty() {
		fmt.Println(it.Item())
		if goPrev {
			it.Prev()
		} else {
			it.Next()
		}
	}
}

func checkLsmSearch(lsm *Lsm) error {
	exp := [][]interface{}{
		{&testEntryKey{str: "23450"}, true},
		{&testEntryKey{str: "0"}, false},
		{&testEntryKey{str: "10"}, true},
		{&testEntryKey{str: "100"}, false},
		{&testEntryKey{str: "111"}, false},
		{&testEntryKey{str: "11130"}, true},
	}
	for _, v := range exp {
		e, err := lsm.Search(v[0].(EntryKey))
		if err != nil {
			return err
		}
		if v[1].(bool) && (e == nil || e.(*testEntry).str != v[0].(*testEntryKey).str) {
			return fmt.Errorf("Expected entry %v not found, e=%v", v[0], e)
		}
		if !v[1].(bool) && e != nil {
			return fmt.Errorf("Entry was found, when was not expected")
		}
	}
	return nil
}

func verifyItEntry(it *Iter, str string) {
	if it.Error() != nil {
		panic("failed to get LSM iter: " + it.Error().Error())
	}
	if it.Result == nil {
		panic("got nil LSM iter")
	}
	if it.Result.(*testEntry).str != str {
		panic(fmt.Sprintf("got incorrect result: %v, exp: %v", it.Result.(*testEntry).str, str))
	}
}

// check that iterator correctly behaves when new entries added
func TestLsmRestart(t *testing.T) {
	beforeTest()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	fatalOnErr(lsm.Insert(&testEntry{data: 0, str: "0"}))

	it := lsm.Find(GE, nil)
	verifyItEntry(it, "0")

	size := 50000
	for n := 0; n < size; n++ {
		d := &testEntry{data: uint32(n * 10), str: strconv.Itoa(n * 10)}
		fatalOnErr(lsm.Insert(d))
	}

	fatalOnErr(lsm.Insert(&testEntry{data: 1, str: "01"}))

	it.Next()
	verifyItEntry(it, "01")

	it.Next()
	verifyItEntry(it, "10")

	size = 100000
	for n := 0; n < size; n++ {
		d := &testEntry{data: uint32(5 + n*10), str: strconv.Itoa(5 + n*10)}
		fatalOnErr(lsm.Insert(d))
	}

	fatalOnErr(lsm.Insert(&testEntry{data: 101, str: "101"}))

	it.Next()
	verifyItEntry(it, "100")

	it.Next()
	verifyItEntry(it, "1000")

	it.Next()
	verifyItEntry(it, "10000")

	it.Next()
	verifyItEntry(it, "100000")

	it.Next()
	verifyItEntry(it, "100005")
}

// sequential keys, some marked as deleted
func TestLsm1(t *testing.T) {
	beforeTest()
	rb := createTestRb()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 100000
	for n := 0; n < size; n++ {
		d := &testEntry{data: uint32(n * 10), str: strconv.Itoa(n * 10)}
		if n%2 != 0 {
			fatalOnErr(lsm.Insert(d))
		} else {
			fatalOnErr(lsm.Remove(d))
		}
		if !d.LsmDeleted() {
			rb.Insert(d)
		}
	}
	err := checkLsm(lsm, rb)
	if err != nil {
		t.Error(err)
	}
	err = checkLsmSearch(lsm)
	if err != nil {
		t.Error(err)
	}
}

// random keys, sometimes with delete
func TestLsm2(t *testing.T) {
	beforeTest()
	defer afterTest()
	rb := createTestRb()
	lsm := createTestLsm()
	defer lsm.WaitMergeDone()

	size := 100000
	for n := 0; n < size; n++ {
		id := rand.Intn(size)
		d := &testEntry{data: uint32(id), str: strconv.Itoa(id)}
		if rand.Intn(2) != 0 {
			fatalOnErr(lsm.Insert(d))
			rb.InsertOrReplace(d, true)
		} else {
			fatalOnErr(lsm.Remove(d))
			rb.Remove(d.GetKey())
		}
	}
	err := checkLsm(lsm, rb)
	if err != nil {
		t.Error(err)
	}

	// Merge may still be in progress here so we need to wait for it to finish to
	// write correct descriptor
	fatalOnErr(lsm.Flush())

	// check that WriteDesc / ReadDesc works
	b := NewSerializeBuf(100)
	lsm.WriteDesc(b)
	lsm.debug("saved desc %v bytes", b.Len())

	// lsm2 shares same storage, but opens via ReadDesc, so may reload C0 and have to initialize properly all Ctrees
	lsm2 := createTestLsm()
	err = lsm2.ReadDesc(NewDeserializeBuf(b.Bytes()))
	fatalOnErr(err)
	lsm.debug("read desc: restored C0 with %v entries", lsm2.C0.getRoot().items.Count())

	err = checkLsm(lsm2, rb)
	if err != nil {
		t.Error(err)
	}

}

// test for Search() exact match when removed items present
func TestLsmSearch(t *testing.T) {
	beforeTest()
	rb := createTestRb()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 1000
	for n := 0; n < size; n++ {
		fatalOnErr(lsm.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n)}))
		rb.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n)})
	}
	for n := 0; n < size; n += 4 {
		fatalOnErr(lsm.Remove(&testEntry{data: uint32(n), str: strconv.Itoa(n)}))
		fatalOnErr(lsm.Remove(&testEntry{data: uint32(n + 1), str: strconv.Itoa(n + 1)}))
		rb.Remove(&testEntryKey{str: strconv.Itoa(n)})
		rb.Remove(&testEntryKey{str: strconv.Itoa(n + 1)})
	}
	err := checkLsm(lsm, rb)
	if err != nil {
		t.Error(err)
	}

	e, err := lsm.Search(&testEntryKey{str: "99"})
	fatalOnErr(err)
	assert(e.(*testEntry).str == "99")
	e, err = lsm.Search(&testEntryKey{str: "100"})
	fatalOnErr(err)
	assert(e == nil)
	e, err = lsm.Search(&testEntryKey{str: "101"})
	fatalOnErr(err)
	assert(e == nil)
	e, err = lsm.Search(&testEntryKey{str: "102"})
	fatalOnErr(err)
	assert(e.(*testEntry).str == "102")
	e, err = lsm.Search(&testEntryKey{str: "103"})
	fatalOnErr(err)
	assert(e.(*testEntry).str == "103")
	e, err = lsm.Search(&testEntryKey{str: "104"})
	fatalOnErr(err)
	assert(e == nil)

	e, err = lsm.Search(&testEntryKey{str: "999"})
	fatalOnErr(err)
	assert(e.(*testEntry).str == "999")
	e, err = lsm.Search(&testEntryKey{str: "1001"})
	fatalOnErr(err)
	assert(e == nil)
}

// test for Search() on large LSM with multiple dir node levels
func TestLargeLsmSearch(t *testing.T) {
	beforeTest()
	rb := createTestRb()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 40 + rand.Intn(100)
	keyLen := 100000
	for n := 0; n < size; n++ {
		fatalOnErr(lsm.Insert(&testEntry{data: uint32(n), str: randomStrKey(n, keyLen)}))
		rb.Insert(&testEntry{data: uint32(n), str: randomStrKey(n, keyLen)})
	}
	err := checkLsm(lsm, rb)
	if err != nil {
		t.Error(err)
	}

	// note: should fix mergeall
	/*
	if rand.Int()%2 == 0 {
		err = lsm.MergeAll()
		fatalOnErr(err)
	}*/

	lsm.cfg.Log.Infoln("Inserted")
	lsm.WaitMergeDone()
	lastCtree := lsm.Ctree[len(lsm.Ctree)-1]
	assert(atomic.LoadInt64(&lastCtree.Stats.DirsSize) != lastCtree.RootSize)

	e, err := lsm.Search(&testEntryKey{str: randomStrKey(2, keyLen)})
	fatalOnErr(err)
	assert(e.(*testEntry).str == randomStrKey(2, keyLen))
	e, err = lsm.Search(&testEntryKey{str: randomStrKey(size-1, keyLen)})
	fatalOnErr(err)
	assert(e.(*testEntry).str == randomStrKey(size-1, keyLen))
	e, err = lsm.Search(&testEntryKey{str: randomStrKey(size/2, keyLen)})
	fatalOnErr(err)
	assert(e.(*testEntry).str == randomStrKey(size/2, keyLen))
	e, err = lsm.Search(&testEntryKey{str: randomStrKey(size, keyLen)})
	fatalOnErr(err)
	assert(e == nil)
}

func TestLsmFindReverse(t *testing.T) {
	beforeTest()
	lsm := createTestLsm()
	rb := createTestRb()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 50000
	for n := 1; n <= size; n++ {
		if rand.Int()%2 == 0 {
			rb.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n * 3)})
			fatalOnErr(lsm.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n * 3)}))
		}
	}

	err := checkIteratorsEqual(lsm.Find(LE, &testEntryKey{str: "150000"}), rb.Find(LE, &testEntryKey{str: "150000"}), true)
	fatalOnErr(err)
	err = checkIteratorsEqual(lsm.Find(LE, &testEntryKey{str: "0"}), rb.Find(LE, &testEntryKey{str: "0"}), true)
	fatalOnErr(err)

	tests := 10
	for i := 0; i < tests; i++ {
		key := &testEntryKey{str: strconv.Itoa(rand.Intn(size * 3))}
		op := LE
		if rand.Int()%2 == 0 {
			op = LT
		}

		lsmIt := lsm.Find(op, key)
		rbIt := rb.Find(op, key)
		err = checkIteratorsEqual(lsmIt, rbIt, true)
		fatalOnErr(err)
	}
}

func TestLsmFindRange(t *testing.T) {
	beforeTest()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 50000
	for n := 0; n < size; n++ {
		fatalOnErr(lsm.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n)}))
	}

	froms := []string{"111", "123", "999", "666666", "99"}
	tos := []string{"333", "123", "888", "99999999", "11"}

	for i := 0; i < len(froms); i++ {
		from := &testEntryKey{froms[i]}
		to := &testEntryKey{tos[i]}
		it := lsm.Find(GE, from)
		it1 := lsm.FindRange(GE, from, to)
		for !it1.Empty() {
			r := it.Result
			r1 := it1.Result
			if testEntryCmp(r, r1) != 0 {
				t.Fatal("Not matching entries: ", r, r1)
			}
			if testEntryKeyCmp(r, to) > 0 {
				t.Fatal("Not in range element found: ", r, " not in [", from, ", ", to, "]")
			}
			it.Next()
			it1.Next()
			fatalOnErr(it.Error())
			fatalOnErr(it1.Error())
		}

		if !it.Empty() {
			if testEntryKeyCmp(it.Result, to) <= 0 {
				t.Fatal("FindRange iterator didn't find element: ", it.Result)
			}
		}
	}
}

func TestLsmLimit(t *testing.T) {
	beforeTest()
	lsm := createTestLsm()
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	size := 50000
	for n := 0; n < size; n++ {
		fatalOnErr(lsm.Insert(&testEntry{data: uint32(n), str: strconv.Itoa(n)}))
	}

	limit := 13
	cnt := 0
	it := lsm.Find(LT, &testEntryKey{str: "9"}).Limit(limit)
	for ; !it.Empty(); it.Next() {
		cnt++
	}
	fatalOnErr(it.Error())
	if cnt != limit {
		t.Fatalf("cnt [%v] != limit [%v]", cnt, limit)
	}
}


