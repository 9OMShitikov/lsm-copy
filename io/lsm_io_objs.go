package io

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/neganovalexey/search/cache"
)

// ObjectGc is a garbage collector for objects interface
type ObjectGc interface {
	// DeleteObject puts an object to GC list. Returns true if object is new.
	DeleteObject(objName string) bool
}

// ObjectGcTest is empty implementation for ObjectGc interface for testing usage
type ObjectGcTest struct{}

// nolint
func (*ObjectGcTest) DeleteObject(objName string) bool { return true }

// LsmPrefetchSize is a size of the object storage read and cached block
// used when reading lsm pages
//
// WARN: must not be changed when lsm reads are in-progress
var LsmPrefetchSize = int64(128 * 1024)

const lsmMergeChunkSize = 5 * 1024 * 1024

// nolint: unused, deadcode
func assertCloudLsmIoInterfaces() {
	var _ LsmIo = (*LsmCloudIo)(nil)
}

// LsmCloudIo is an implementation of LsmIo to use with object storage
// This IO implementation uses name of the lsm to construct object names
// to store ctrees, so be sure that each lsm in your application uses
// different LsmCloudIo instances with different names in case the same
// underlying object storage used
type LsmCloudIo struct {
	// base path to be used to prefix object names
	basePath string
	// name of the lsm for which this IO is used
	lsmName string
	storage MetaObjStorage

	// prefetch cache
	cache cache.BasicCache

	log *logrus.Logger

	gc ObjectGc

	ctx context.Context
}

// NewLsmCloudIo creates new instance of lsm io with object storage backend specified
func NewLsmCloudIo(ctx context.Context, basePath string, lsmName string, storage MetaObjStorage, cache cache.BasicCache, log *logrus.Logger, gc ObjectGc) *LsmCloudIo {
	if log == nil {
		log = logrus.New()
	}

	return &LsmCloudIo{
		basePath: basePath,
		lsmName:  lsmName,
		storage:  storage,
		cache:    cache,
		log:      log,
		gc:       gc,
		ctx:      ctx,
	}
}

// Ctx returns current io context
func (lio *LsmCloudIo) Ctx() context.Context {
	return lio.ctx
}

// ReadPages reads part of the ctree from object storage
func (lio *LsmCloudIo) ReadPages(ctreeID CtreeID, offs, sz int64) (data []byte, err error) {
	reader := lio.newPagesReader(lio.ctx, ctreeID, offs)
	data = make([]byte, sz)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't read [%s] at [%d]", lio.ctreeObjectName(ctreeID), offs)
	}

	err = reader.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't read [%s] at [%d]", lio.ctreeObjectName(ctreeID), offs)
	}

	return
}

// FreeBlocks does nothing in case of cloud based Io
func (lio *LsmCloudIo) FreeBlocks(blocks []CTreeNodeBlock) (err error) {
	for _, b := range blocks {
		lio.gc.DeleteObject(GetCtreeObjectName(MetadataDir, lio.lsmName, b.CtreeID))
	}
	return
}

type lsmPagesChunk []byte

func (c lsmPagesChunk) OnCacheDropped() {}

func (lio *LsmCloudIo) newPagesReader(ctx context.Context, ctreeID CtreeID, offs int64) io.ReadCloser {
	return &lsmPagesReader{
		lio:      lio,
		cache:    lio.cache,
		cacheKey: prefetchKey{ctreeID},
		curOffs:  offs,
		ctx:      ctx,
	}
}

// prefetchKey used in cache operations to distinguish cached chunks with c-trees blocks
// with possibly equal offsets
type prefetchKey struct {
	ctreeID CtreeID
}

// lsmPagesReader implements io.ReadCloser and used to read c-tree pages with additional
// help of prefetch cache
type lsmPagesReader struct {
	lio      *LsmCloudIo
	cache    cache.BasicCache
	cacheKey prefetchKey
	curOffs  int64
	ctx      context.Context
}

func (r *lsmPagesReader) Read(p []byte) (n int, err error) {
	chunkOffs := r.curOffs - r.curOffs%LsmPrefetchSize
	skip := r.curOffs % LsmPrefetchSize

	for n < len(p) {
		chunk, err := r.readChunk(chunkOffs)
		if err != nil {
			r.curOffs += int64(n)
			return n, err
		}
		n += copy(p[n:], chunk[skip:])
		skip = 0
		chunkOffs += LsmPrefetchSize
	}

	r.curOffs += int64(n)
	return
}

func (r *lsmPagesReader) readChunk(chunkOffs int64) (chunk []byte, err error) {
	obj, err := r.cache.Read(r.cacheKey, chunkOffs, func() (obj cache.CachedObj, size int64, err error) {
		// reading chunk from storage
		objName := r.lio.ctreeObjectName(r.cacheKey.ctreeID)
		bs, err := ReadObject(r.ctx, r.lio.storage, objName, chunkOffs, LsmPrefetchSize)
		r.lio.log.WithField("io", "lsmPagesReader").
			WithField("object", objName).
			WithField("err", err).
			Debugf("Fetch chunk at [%v] of size [%v] from object", chunkOffs, LsmPrefetchSize)
		return lsmPagesChunk(bs), int64(len(bs)), err
	})

	if err != nil {
		return
	}

	chunk = obj.(lsmPagesChunk)
	return
}

func (r *lsmPagesReader) Close() error {
	return nil
}

// LsmMergeCloudWriter implements uploading ctrees merge result to object storage
// chunk by chunk
type LsmMergeCloudWriter struct {
	lio *LsmCloudIo
	// channel used to finish merge result upload
	done chan error
	// pipe writer and pipe reader used to write the merge pages
	// so at each pages write bytes are written to pw PipeWriter
	// and in separate goroutine that data is read from pr PipeReader
	pw *io.PipeWriter
	pr *io.PipeReader
	// write offset (append only) to be returned on the next WritePages call
	offs int64
	// name of resulting ctree object in storage
	mergeResultObject string
}

// Close finalizes merge and writes merged ctree to the storage.
// Must be called after all WritePages calls finished successfully
// Must be called if no WritePages calls was issued at all
// May not be called if one of WritePages calls failed
// TODO: maybe not needed ctree objects may be reported the similar way as it is done
//       in LsmIoFile with Ralloc; But for now we just use external garbage collection.
func (mw *LsmMergeCloudWriter) Close() (err error) {
	return mw.close(nil)
}

// Abort closes writer with abort error so merge result object is not written to the storage
// In case merge writer already returned error from WritePages this method does nothing
func (mw *LsmMergeCloudWriter) Abort() (err error) {
	abortErr := errors.New("CloudMergeWriterAbort")
	err = mw.close(abortErr)
	if err == abortErr || errors.Cause(err) == context.Canceled {
		err = nil
	}
	return
}

func (mw *LsmMergeCloudWriter) close(closeErr error) (err error) {
	// closing write end of the pipe so read end gets closeErr and writeGor stops
	// mw.pr and mw.done will be closed in writeGor
	_ = mw.pw.CloseWithError(closeErr) // no error checks because PipeWriter.CloseWithError always returns nil

	// waiting for writeGor to finish, if not yet; here we get error, which has
	// occurred during uploading of the object, if any; in case closeErr passed to close
	// is not nil when we also might get this error here too
	err = <-mw.done

	mw.lio.log.WithField("io", "LsmMergeCloudWriter").
		WithField("object", mw.lio.lsmName).
		WithField("err", err).
		Debugf("merge close object")
	return
}

// WritePages writes given data buffer. It is not safe to call it from several goroutines for now.
// If any call to WritePages fails with error Finish() may not be called.
func (mw *LsmMergeCloudWriter) WritePages(buf []byte) (offs int64, err error) {
	// this write call blocks until reader reads data or read end is closed with an error
	// which is the case if object uploading fails: in this situation we will get this error here
	n, err := mw.pw.Write(buf)
	offs = mw.offs
	mw.offs += int64(n)
	return offs, err
}

func (mw *LsmMergeCloudWriter) writeGor() {
	// this call blocks until mw.pr returns error (EOF or other) or error occurres during objs operation
	o, err := mw.lio.storage.NewWriteObject(mw.mergeResultObject)
	if err == nil {
		err = o.WriteChunked(mw.lio.ctx, mw.pr, lsmMergeChunkSize)
	}
	// any consequent write to the pipe are not expected: we either finished successfully or failed with
	// non nil error, so lets close reader end with that error so writer, in case it still expects to write
	// something, fails with this error too
	_ = mw.pr.CloseWithError(err) // no error checks because PipeReader.CloseWithError always returns nil
	// finally tell Close() or Abort() goroutine that uploading has finished
	mw.done <- err
	close(mw.done)
}

// NewMergeWriter creates new writer for uploading merge result of two ctrees to object storage
// Because this call spawns new goroutine user must properly use MergeWriter api to avoid dangling
// goroutine: see comments on LsmMergeCloudWriter methods.
func (lio *LsmCloudIo) NewMergeWriter(dstCtreeID CtreeID) MergeWriter {
	newToName := lio.ctreeObjectName(dstCtreeID)
	pr, pw := io.Pipe()

	mw := &LsmMergeCloudWriter{
		lio:               lio,
		done:              make(chan error),
		pr:                pr,
		pw:                pw,
		mergeResultObject: newToName,
	}

	// assuming that if merge writer was created then it will be properly used
	// and WritePages() and/or Finish() will be called
	go mw.writeGor()

	return mw
}

func (lio *LsmCloudIo) ctreeObjectName(ctreeID CtreeID) string {
	assert(ctreeID.DiskGen >= uint64(1)<<32) // verify it's initialized properly
	return GetCtreeObjectName(lio.basePath, lio.lsmName, ctreeID)
}

// GetCtreeObjectName constructs name of the ctree object used to store lsm ctree data
func GetCtreeObjectName(lsmBasePath string, lsmName string, ctreeID CtreeID) string {
	if len(lsmBasePath) > 0 {
		return fmt.Sprintf("%s/%s_%x", lsmBasePath, lsmName, ctreeID.DiskGen)
	}
	return fmt.Sprintf("%s_%x", lsmName, ctreeID.DiskGen)
}
