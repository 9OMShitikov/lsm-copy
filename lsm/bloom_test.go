package lsm

import (
	"crypto/rand"
	"math"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type byteKey struct {
	init []byte
	pos  int
}

func newByteKey(sz int) *byteKey {
	buf := make([]byte, sz)
	_, err := rand.Read(buf)
	fatalOnErr(err)
	return &byteKey{buf, 0}
}

func (b *byteKey) atPos(pos int) *byteKey {
	b.pos = pos
	return b
}

func (b *byteKey) len() int64 {
	return int64(len(b.init))
}

func (b *byteKey) Encode(buf *SerializeBuf) {
	buf.EncodeBuf(b.init)
	buf.EncodeInt64(int64(b.pos))
}

func (b *byteKey) Decode(buf *DeserializeBuf) error {
	panic("implement me")
}

func (b *byteKey) String() string {
	panic("implement me")
}

func (b *byteKey) Size() int {
	return len(b.init) + int(unsafe.Sizeof(*b))
}

func BenchmarkBloomInsertAndExists(b *testing.B) {
	for _, bufSz := range []int{16, 64, 128, 512, 1024, 2048, 4096, 8092, 16384} {
		n := 1000 * 1000
		f := newCtreeBloomDefault(n)
		key := newByteKey(bufSz)
		b.Run("Insert"+strconv.Itoa(bufSz), func(b *testing.B) {
			b.ReportMetric(float64(f.length()), "filter_bytes")
			b.ReportMetric(float64(n), "expected_items")
			b.SetBytes(key.len())
			for i := 0; i < b.N; i++ {
				f.insert(key.atPos(i))
			}
			b.ReportAllocs()
		})
		b.Run("Exists"+strconv.Itoa(bufSz), func(b *testing.B) {
			b.SetBytes(key.len())
			for i := 0; i < b.N; i++ {
				f.exists(key.atPos(i * 2))
			}
			b.ReportAllocs()
		})
	}
}

func TestBloomDefaultFPR(t *testing.T) {
	n := 1000 * 1000
	f := newCtreeBloomDefault(n)
	rounds := n * 3
	key := newByteKey(12)
	fp := 0

	for i := 0; i < n; i++ {
		f.insert(key.atPos(i))
	}
	for i := 0; i < n; i++ {
		require.True(t, f.exists(key.atPos(i)))
	}
	for i := 0; i < rounds; i++ {
		if f.exists(key.atPos(n + i)) {
			fp++
		}
	}
	fprGot := float64(fp) / (float64(rounds))
	fprExpected := math.Pow(1-math.Exp(-float64(bloomDefaultHashes)/float64(bloomDefaultBitsPerExpectedItem)), bloomDefaultHashes)

	t.Logf("real fpr = %.6f, expected fpr = %.6f", fprGot, fprExpected)
	require.Truef(t, fprGot <= 1.2*fprExpected, "too large real fpr")
}

func TestBloomEncodeDecode(t *testing.T) {
	n := 1000 * 1000
	f := newCtreeBloomDefault(n)
	key := newByteKey(12)
	for i := 0; i < n; i++ {
		f.insert(key.atPos(i))
	}

	for _, align := range []bool{false, true} {
		buf := NewSerializeBuf(1)
		f.encode(buf, align)
		ff, err := decodeCtreeBloom(NewDeserializeBuf(buf.Bytes()))
		require.NoError(t, err)
		require.Equal(t, f, ff)
	}
}
