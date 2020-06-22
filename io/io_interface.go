package io

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
)

// Io is a write/read interface for data
type Io interface {
	// CreateLsmIo initializes lsm io based on generic io
	CreateLsmIo(lsmFolder, lsmName string, cache cache.BasicCache, gc ObjectGc) LsmIo

	GetMetaObjStorage() MetaObjStorage

	Log() *logrus.Logger
}

// Config describes underlying storage
type Config struct {
	// Base directory
	Root string

	Ctx context.Context
	Log *logrus.Logger
}

// New creates io based on given Io config
func New(cfg Config) (io Io, err error) {
	if cfg.Log == nil {
		cfg.Log = logrus.New()
	}
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}

	storage, err := NewFSObjStorage(cfg)
	if err != nil {
		return nil, err
	}
	return NewObjsIo(cfg.Ctx, storage, cfg.Log), nil
}
