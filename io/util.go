package io

import (
	"fmt"
	"io"
)

// ReadFull tries to read len(buf) bytes from reader
// If everything is ok and there is enough data n = len(buf) and err = nil is returned
// If EOF occurred then n < len(buf) and err = nil is returned
// If not-EOF error occurred, than returned error is not nil and n = [number of bytes read]
func ReadFull(r io.Reader, buf []byte) (n int, err error) {
	var n1 int
	for n < len(buf) && err == nil {
		n1, err = r.Read(buf[n:])
		assert(err != nil || n1 > 0)
		n += n1
	}
	if err == io.EOF {
		err = nil
	}
	return
}

func assert(cond bool) {
	if !cond {
		panic("assert")
	}
}

func panicIf(cond bool, f string, args ...interface{}) {
	if cond {
		panic(fmt.Sprintf(f, args...))
	}
}

// nolint: unused, deadcode
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
