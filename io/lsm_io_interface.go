package io

import (
	"context"
)

// CtreeID identifies ctree in the whole set of lsms and ctrees
type CtreeID struct {
	LsmID   uint32 // identifies lsm
	DiskGen uint64 // identifies ctree inside lsm
}

// CTreeNodeBlock is a lsm-data block identifier
type CTreeNodeBlock struct {
	CtreeID CtreeID
	Offs    int64
	Size    int64
}

// MergeWriter is an interface which is used during ctrees merge -- the only
// stage when disk writes happen in lsm tree
type MergeWriter interface {
	// WritePages appends given data to merge resulting ctree. For example it may
	// write given buffer to underlying file on local filesystem.
	WritePages(buf []byte) (offs int64, err error)

	// Close finalizes merge stage. This method must be called for any MergeWriter
	// created with LsmIo.NewMergeWriter() call in case all WritePages calls returned
	// without error
	Close() (err error)

	// Abort stops MergeWriter operation (if any) and tries hard to rollback any already
	// performed page writes
	//
	// Abort is an alternative way to close merge writer. Either Close() or Abort() must
	// be called when user wants to finalize merge operation
	Abort() (err error)
}

// LsmIo is a block-based IO interface, it provides methods for writing
// lsm to storage and reading it from it.
type LsmIo interface {
	// ReadPages reads sz bytes starting from given offset from a ctree data blocks, identified
	// by ctree id
	ReadPages(ctreeID CtreeID, off int64, sz int64) (data []byte, err error)
	// FreeBlocks should be used to tell to underlying storage that some parts
	// of lsm are not needed anymore and may be reused
	FreeBlocks(blocks []CTreeNodeBlock) (err error)
	// NewMergeWriter creates new writer object for writing the merge result
	// specified id is used to identify resulting ctree (merge result)
	NewMergeWriter(dstCtreeID CtreeID) MergeWriter
	// Returns I/O context
	Ctx() context.Context
}
