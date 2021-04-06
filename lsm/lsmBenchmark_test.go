package lsm

import (
	"strconv"
	"strings"
	"testing"
)

// benchmark for insert
var benchInsertSizes = [] int {100000, 300000, 500000, 700000, 900000, 1100000, 1300000, 1500000, 2000000}
var benchInsertKeyLen = 100
func lsmInsertBench(b *testing.B, stringGenerator func(int, int) string,
	sizes []int, keyLen int, keysOrder string) {
	TestModeOff()
	beforeTest()

	benchInsert := func(b* testing.B, size int, sizeAmpCheck bool) {
		b.StopTimer()
		lsm := createTestLsmWithBloom(true)
		lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = sizeAmpCheck
		b.StartTimer()
		for i := 0; i < size; i++ {
			lsm.Insert(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
		}
		lsm.WaitMergeDone()
	}

	for _, size := range sizes {
		b.Run("insert_" + keysOrder + "_" +
			strconv.Itoa(size) + "_entries_without_size_amplification_check", func(b* testing.B) {
			benchInsert(b, size, false)
		})
	}

	for _, size := range sizes {
		b.Run("insert_"+ keysOrder + "_" +
			strconv.Itoa(size) + "_entries_with_size_amplification_check", func(b* testing.B) {
			benchInsert(b, size, true)
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
	lsmInsertBench(b, randomStrKey, benchInsertSizes, benchInsertKeyLen, "random_keys_order")
}

func BenchmarkLsmInsertAscendingKeys(b *testing.B) {
	lsmInsertBench(b, ascendingStrKey, benchInsertSizes, benchInsertKeyLen, "ascending_keys_order")
}

func BenchmarkLsmSearch(b *testing.B) {
	TestModeOff()
	beforeTest()

	sizes := [] int {100, 200, 300, 500, 700}
	keyLen := 10000
	keys := make([]string, sizes[len(sizes) - 1])
	for i := range keys {
		keys[i] = randomStrKey(i, keyLen)
	}

	for _, size := range sizes {
		b.Run("search_"+strconv.Itoa(size)+"_entries_without_size_amplification_check", func(b* testing.B) {
			lsm := createTestLsmWithBloom(true)
			lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = false
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
