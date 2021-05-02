package testutil

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	aio "github.com/neganovalexey/search/io"
)

// GetTestIoConfig constructs IO config with specified logger
func GetTestIoConfig(log *logrus.Logger) aio.Config {
	return aio.Config{
		Root: "test-folder-" + RandomStrKey(int(time.Now().UnixNano()), 16),
		Log:  log,
		Ctx:  context.Background(),
	}
}

// RandomStrKey returns stable random key for given n of length sz
// nolint: unparam
func RandomStrKey(n, sz int) (res string) {
	for len(res) < sz {
		buf := [8]byte{}
		binary.BigEndian.PutUint64(buf[:], uint64(n))
		h := sha512.Sum512(buf[:])
		res = res + base64.StdEncoding.EncodeToString(h[:])
	}
	return res[:sz]
}

// AscendingStrKey returns stable ascending keys for given n of length sz
func AscendingStrKey(n int, sz int) string {
	if len(strconv.Itoa(n)) > sz {
		panic("too small string size")
	}
	return fmt.Sprintf("%0*d\n", sz, n)
}

