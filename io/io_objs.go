package io

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
)

// MetadataDir is the directory, where all lsm ctrees (metadata) is stored
// in case of object storage Io
const MetadataDir = "metadata"

// ObjsIo implements Io using object storage interface
type ObjsIo struct {
	fName string

	mStorage MetaObjStorage

	log *logrus.Logger

	ctx    context.Context
	cancel func()
}

// NewObjsIo creates new Object Storage Based IO with underlying object storage
// interface given
func NewObjsIo(ctx context.Context, metadataStorage MetaObjStorage, log *logrus.Logger) Io {
	ctx, cancel := context.WithCancel(ctx)
	return &ObjsIo{
		mStorage: metadataStorage,
		log:      log,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Log returns io logrus
func (io *ObjsIo) Log() *logrus.Logger {
	return io.log
}

// CreateLsmIo for objs based io returns objs based lsm
// Passed cache is used for storing prefetched lsm nodes
func (io *ObjsIo) CreateLsmIo(lsmFolder, lsmName string, cache cache.BasicCache, gc ObjectGc) LsmIo {
	fullLsmDir := io.fName + "/" + MetadataDir
	if lsmFolder != "" {
		fullLsmDir += lsmFolder
	}
	lio := NewLsmCloudIo(io.ctx, fullLsmDir, lsmName, io.mStorage, cache, io.log, gc)
	return lio
}

// GetMetaObjStorage returns underlying object storage protected with Io open mode
func (io *ObjsIo) GetMetaObjStorage() MetaObjStorage {
	return io.mStorage
}
