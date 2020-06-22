package io

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// FSObjStorage is a local file system based object storage simulation
type FSObjStorage struct {
	root string
	log  *logrus.Logger
}

// NewFSObjStorage simply creates new file system based object storage implementation
// which initialized inside directory, specified by rootPath
func NewFSObjStorage(cfg Config) (storage *FSObjStorage, err error) {
	rootPath := path.Clean(cfg.Root)
	if cfg.Root == "" {
		rootPath = ""
	}
	return &FSObjStorage{root: rootPath, log: cfg.Log}, nil
}

// Log returns io logrus
func (fs *FSObjStorage) Log() *logrus.Logger {
	return fs.log
}

// BaseURL return filesystem base path
func (fs *FSObjStorage) BaseURL() string {
	url := "file://" + fs.root
	if fs.root != "/" {
		url = url + "/"
	}
	return url
}

// ListObjects just lists the {name} part for files (not dirs) which name is {root}/{path}/{name}
func (fs *FSObjStorage) ListObjects(ctx context.Context, path string, fn ListObjectsFunc) (err error) {
	dir := filepath.Join(fs.root, path)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) { // no objects in "non-existing" dir
			err = nil
		}
		return
	}

	for i := range infos {
		if !infos[i].IsDir() {
			err = fn(infos[i].Name(), infos[i].Size(), infos[i].ModTime())
			if err == ErrStopListing {
				return nil
			}
			if err != nil {
				return
			}
		}
	}

	return
}

// ReadObjectAt reads object at offset
func (fs *FSObjStorage) ReadObjectAt(ctx context.Context, path string, offs int64, buf []byte) (n int, err error) {
	filename := filepath.Join(fs.root, path)

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	finfo, err := os.Stat(filename)
	if os.IsNotExist(err) || (err == nil && finfo.IsDir()) {
		err = errors.Wrapf(ErrObjNotFound, "ReadObjectAt(%s)", path)
	}
	if err != nil {
		return
	}

	// nolint: gosec
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	n, err = file.ReadAt(buf, offs)
	if err == io.EOF {
		err = nil
	}
	if errClose := file.Close(); err == nil {
		err = errClose
	}

	return
}

type fsObjStorageWriteObject struct {
	path string
	fs   *FSObjStorage
}

func (wo *fsObjStorageWriteObject) Write(ctx context.Context, data io.ReadSeeker) (err error) {
	_, err = wo.writeChunked(ctx, data, 0)
	return
}

// WriteChunked for FS-based storage simply copies given data to the file using buffer of chunkSize length
func (wo *fsObjStorageWriteObject) WriteChunked(ctx context.Context, data io.Reader, chunkSize int) (err error) {
	_, err = wo.writeChunked(ctx, data, chunkSize)
	if err != nil {
		return
	}
	return
}

func (wo *fsObjStorageWriteObject) writeChunked(ctx context.Context, data io.Reader, chunkSize int) (total int64, err error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	filename := filepath.Join(wo.fs.root, wo.path)

	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		return
	}

	if chunkSize > 0 {
		buffer := make([]byte, chunkSize)
		total, err = io.CopyBuffer(file, data, buffer) // buffered io
	} else {
		total, err = io.Copy(file, data)
	}

	errClose := file.Close()
	if err == nil {
		err = errClose
	}

	if err != nil {
		_ = os.Remove(filename)
	}

	return
}

// NewWriteObject makes WriteObject struct from path
func (fs *FSObjStorage) NewWriteObject(path string) (WriteObject, error) {
	return fs.newWriteObject(path), nil
}

func (fs *FSObjStorage) newWriteObject(path string) *fsObjStorageWriteObject {
	return &fsObjStorageWriteObject{path: path, fs: fs}
}

// DeleteObject simply removes file from disk
func (fs *FSObjStorage) DeleteObject(ctx context.Context, path string) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	filename := filepath.Join(fs.root, path)

	err = os.Remove(filename)
	if os.IsNotExist(err) {
		err = nil
	}
	return
}
