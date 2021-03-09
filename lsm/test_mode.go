package lsm

import (
	"math/rand"
)

var savedDefaults MergeConfig
// TestModeOn enables integrity checks and tunes some configuration variables
// to be suitable for tests and debugging
func TestModeOn() {
	verifyLsmNodesImmutable = true
	savedDefaults = defaultMergeCfg
	defaultMergeCfg.MergeItemsSizeBatch = 1024 + rand.Int()%(1*1024*1024)       // nolint:gosec
	defaultMergeCfg.C0MergeMinThreshold = 32*1024 + int64(rand.Int()%(32*1024)) // nolint:gosec
	defaultMergeCfg.C0MergeMaxThreshold = defaultMergeCfg.C0MergeMinThreshold
	defaultMergeCfg.MergeDirsFlushThreshold = 2 + rand.Int()%10 // nolint:gosec // at least 2 entries needed to converge
}

// TestModeOff disables some test only suitable settings, if any
func TestModeOff() {
	verifyLsmNodesImmutable = false
	defaultMergeCfg = savedDefaults
}
