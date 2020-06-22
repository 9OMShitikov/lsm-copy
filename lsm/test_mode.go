package lsm

import (
	"math/rand"
)

// TestModeOn enables integrity checks and tunes some configuration variables
// to be suitable for tests and debugging
func TestModeOn() {
	verifyLsmNodesImmutable = true

	MergeItemsSizeBatch = 1024 + rand.Int()%(1*1024*1024)       // nolint:gosec
	C0MergeMinThreshold = 32*1024 + int64(rand.Int()%(32*1024)) // nolint:gosec
	C0MergeMaxThreshold = C0MergeMinThreshold
	MergeDirsFlushThreshold = 2 + rand.Int()%10 // nolint:gosec // at least 2 entries needed to converge
}

// TestModeOff disables some test only suitable settings, if any
func TestModeOff() {
	verifyLsmNodesImmutable = false
	MergeItemsSizeBatch = defaultMergeItemsSizeBatch
	C0MergeMinThreshold = defaultC0MergeMinThreshold
	C0MergeMaxThreshold = defaultC0MergeMaxThreshold
	MergeDirsFlushThreshold = defaultMergeDirsFlushThreshold
}
