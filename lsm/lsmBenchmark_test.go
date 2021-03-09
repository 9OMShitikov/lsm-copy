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

	sizes := [] int {100, 200, 300, 500, 700}
	keyLen := 10000

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
			strconv.Itoa(size) + "_entries", func(b* testing.B) {
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

func BenchmarkLsm_Insert_RandomKeys(b *testing.B) {
	lsmInsertBench(b, randomStrKey, "random_keys_order")
}

func BenchmarkLsm_Insert_AscendingKeys(b *testing.B) {
	lsmInsertBench(b, ascendingStrKey, "random_keys_order")
}

func BenchmarkLsm_Search(b *testing.B) {
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
