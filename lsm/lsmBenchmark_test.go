package lsm

import (
	"strconv"
	"strings"
	"testing"
)

// benchmark for insert
func lsmInsertBench(b *testing.B, stringGenerator func(int, int) string, keysOrder string) {
	TestModeOff()
	beforeTest()
	lsm := createTestLsmWithBloom(true)
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	sizes := [] int {700000, 800000, 900000, 1000000}
	keyLen := 100
	lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = false

	benchInsert := func(b* testing.B, size int) {
		for i := 0; i < size; i++ {
			lsm.Insert(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
		}
		lsm.WaitMergeDone()
		b.StopTimer()
		lsm.Flush()
		lsm.WaitMergeDone()
		b.StartTimer()
	}

	for _, size := range sizes {
		b.Run("insert_" + keysOrder + "_" +
			strconv.Itoa(size) + "_entries_without_size_amplification_check", func(b* testing.B) {
			benchInsert(b, size)
		})
	}

	lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = true
	for _, size := range sizes {
		b.Run("insert_"+ keysOrder + "_" +
			strconv.Itoa(size) + "_entries_with_size_amplification_check", func(b* testing.B) {
			benchInsert(b, size)
		})
	}
}

func ascendingStrKey(n int, sz int) string {
	res := strconv.Itoa(n)
	if len(res) > sz {
		panic("too small string size")
	}
	return strings.Repeat("0", sz - len(res)) + res
}

func BenchmarkLsmInsertRandomKeys(b *testing.B) {
	lsmInsertBench(b, randomStrKey, "random_keys_order")
}

func BenchmarkLsmInsertAscendingKeys(b *testing.B) {
	lsmInsertBench(b, ascendingStrKey, "ascending_keys_order")
}

func BenchmarkLsmSearch(b *testing.B) {
	TestModeOff()
	beforeTest()
	lsm := createTestLsmWithBloom(true)
	defer func() {
		fatalOnErr(lsm.Flush())
		afterTest()
	}()

	sizes := [] int {100, 200, 300, 500, 700}
	keyLen := 10000
	keys := make([]string, sizes[len(sizes) - 1])
	for i := range keys {
		keys[i] = randomStrKey(i, keyLen)
	}

	lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = false
	for _, size := range sizes {
		b.Run("search_"+strconv.Itoa(size)+"_entries_without_size_amplification_check", func(b* testing.B) {
			b.StopTimer()
			for i := 0; i < size; i++ {
				lsm.Insert(&testEntry{data: uint32(i), str: keys[i]})
			}
			lsm.WaitMergeDone()
			b.StartTimer()
			for i := 0; i < size; i++ {
				lsm.Search(&testEntryKey {str: keys[i]})
			}
			b.StopTimer()
			lsm.Flush()
			lsm.WaitMergeDone()
			b.StartTimer()
		})
	}
}
