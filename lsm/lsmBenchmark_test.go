package lsm

import (
	"github.com/neganovalexey/search/testutil"
	"math/rand"
	"strconv"
	"testing"
)

// benchmark for insert
var benchInsertSizes = [] int {100000, 300000, 500000, 700000, 900000, 1100000, 1300000, 1500000, 2000000}
var benchInsertKeyLen = 100

func runOnSizes (b* testing.B, sizes[]int, sizeAmpCheck bool,
				 runFunc func(b* testing.B, size int, ampCheck bool)) {
	for _, size := range sizes {
		b.Run(strconv.Itoa(size) + "_entries", func(b* testing.B) {
			runFunc(b, size, sizeAmpCheck)
		})
	}
}

func runOnSizesWithTree (b* testing.B, sizes[]int, sizeAmpCheck bool, stringGenerator func(int, int) string,
	keyLen int, runFunc func(b* testing.B, size int, tree *Lsm)) {
	for _, size := range sizes {
		lsm := createTestLsmWithBloom(true)
		for i := 0; i < size; i++ {
			lsm.Insert(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
		}
		lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = sizeAmpCheck
		lsm.WaitMergeDone()
		b.Run(strconv.Itoa(size) + "_entries", func(b* testing.B) {
			runFunc(b, size, lsm)
		})
	}
}

func lsmInsertBench(b *testing.B, stringGenerator func(int, int) string,
	sizes []int, keyLen int) {
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

	b.Run("WithoutSizeAmplificationCheck", func(b* testing.B) {
		runOnSizes(b, sizes, false, benchInsert)
	})


	b.Run("WithSizeAmplificationCheck", func(b* testing.B) {
		runOnSizes(b, sizes, true, benchInsert)
	})
}

func BenchmarkLsm_InsertRandomKeysOrder(b *testing.B) {
	lsmInsertBench(b, testutil.RandomStrKey, benchInsertSizes, benchInsertKeyLen)
}

func BenchmarkLsm_InsertAscendingKeysOrder(b *testing.B) {
	lsmInsertBench(b, testutil.AscendingStrKey, benchInsertSizes, benchInsertKeyLen)
}

var benchInsertAndDeleteSizes = [] int {50000, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 900000}
var benchInsertAndDeleteKeyLen = 100
var benchInsertAndDeleteCycles = 3

func lsmInsertAndDeleteBench(b *testing.B, stringGenerator func(int, int) string,
	sizes []int, keyLen int, cyclesCount int) {
	TestModeOff()
	beforeTest()

	benchInsertAndDelete := func(b* testing.B, size int, sizeAmpCheck bool) {
		b.StopTimer()
		lsm := createTestLsmWithBloom(true)
		lsm.cfg.MergeCfg.EnableSizeAmplificationCheck = sizeAmpCheck
		for i := 0; i < size; i++ {
			lsm.Insert(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
		}
		cycleSize := size / cyclesCount

		b.StartTimer()
		for i := 0; i < cyclesCount; i++ {
			for j := i * cycleSize; j < (i + 1) * cycleSize; j++ {
				lsm.Remove(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
			}
			for j := i * cycleSize; j < (i + 1) * cycleSize; j++ {
				lsm.Insert(&testEntry{data: uint32(i), str: stringGenerator(i, keyLen)})
			}
		}
		lsm.WaitMergeDone()
	}

	b.Run("WithoutSizeAmplificationCheck", func(b* testing.B) {
		runOnSizes(b, sizes, false, benchInsertAndDelete)
	})


	b.Run("WithSizeAmplificationCheck", func(b* testing.B) {
		runOnSizes(b, sizes, true, benchInsertAndDelete)
	})
}

func BenchmarkLsm_InsertAndDelete(b *testing.B) {
	lsmInsertAndDeleteBench(b, testutil.RandomStrKey, benchInsertAndDeleteSizes, benchInsertAndDeleteKeyLen,
		           benchInsertAndDeleteCycles)
}

var benchSearchTreeSizes = [] int {100000, 300000, 500000, 700000, 900000, 1100000, 1300000, 1500000, 2000000}
var benchSearchKeyLen = 100
var benchSearchEntries = 100

func lsmSearchBench(b *testing.B, stringGenerator func(int, int) string,
	sizes []int, keyLen int) {
	TestModeOff()
	beforeTest()

	benchSearch := func(b* testing.B, size int, lsm *Lsm) {
		b.StopTimer()
		rand.Seed(42)
		keys := make([]testEntryKey, benchSearchEntries)
		for i := 0; i < benchSearchEntries; i++ {
			t := rand.Intn(size)
			keys[i] = testEntryKey {str: stringGenerator(t, keyLen)}
		}
		b.StartTimer()
		for i := 0; i < benchSearchEntries; i++ {
			lsm.Search(&keys[i])
		}
	}

	b.Run("WithoutSizeAmplificationCheck", func(b* testing.B) {
		runOnSizesWithTree(b, sizes, false, stringGenerator, keyLen, benchSearch)
	})


	b.Run("WithSizeAmplificationCheck", func(b* testing.B) {
		runOnSizesWithTree(b, sizes, true, stringGenerator, keyLen, benchSearch)
	})
}

func BenchmarkLsm_Search(b *testing.B) {
	lsmSearchBench(b, testutil.RandomStrKey, benchSearchTreeSizes, benchSearchKeyLen)
}
