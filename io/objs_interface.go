package io

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
)

// ListObjectsFunc is a callback used by MetaObjStorage.ListObjects(...) function
// for consuming listed object names
type ListObjectsFunc func(path string, size int64, created time.Time) (err error)

// ErrStopListing should be returned by ListObjectsFunc in case user wants
// to stop listing procedure (MetaObjStorage.ListObject should return immediately)
var ErrStopListing = errors.New("")

// DataObjStorage describes abstract object storage for data objects
type DataObjStorage interface {
	// ListObjects calls specified handler function for each object in storage starting with path prefix
	// So if the storage has an object with name {path}/{name}, where {name} does not contain "/" delimiter,
	// then {name} will be passed to fn
	// If specified list function `fn` returns ErrStopListing or other error, listing process is terminated
	ListObjects(ctx context.Context, path string, fn ListObjectsFunc) (err error)
	// ReadObjectAt reads part of object into given buffer. It uses passed buffer to get desired size
	// It returns number of bytes read and put into the buf. It does not report error in case EOF hit
	ReadObjectAt(ctx context.Context, path string, offs int64, buf []byte) (n int, err error)
	NewWriteObject(path string) (WriteObject, error)

	DeleteObject(ctx context.Context, path string) error // object-not-found errors are suppressed
}

type MetaObjStorage = DataObjStorage

// WriteObject represents object storage object
// to be used for writing.
type WriteObject interface {
	Write(ctx context.Context, data io.ReadSeeker) error
	// WriteChunked writes an object into the object storage part by part so that maximum
	// number of object bytes transferred at once is equal to chunkSize
	WriteChunked(ctx context.Context, data io.Reader, chunkSize int) (err error)
}

// ReadObject reads part of the object from object storage
func ReadObject(ctx context.Context, storage MetaObjStorage, path string, offs int64, len int64) (data []byte, err error) {
	data = make([]byte, len)

	n, err := storage.ReadObjectAt(ctx, path, offs, data)
	if err != nil {
		return nil, err
	}

	if int64(n) < 9*len/10 {
		tmp := make([]byte, n)
		copy(tmp, data)
		data = tmp
	} else {
		data = data[:n]
	}
	return
}
