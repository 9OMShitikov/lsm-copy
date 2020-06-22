package lsm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/spaolacci/murmur3"

	"github.com/neganovalexey/search/io"
)

type bloomWord uint64

type ctreeBloom struct {
	bitset    []bloomWord
	numHashes int
}

const (
	bloomWordBits  = 64
	bloomWordShift = 6

	bloomSupportedMaxCtreeCount = 1000 * 1000 * 1000

	bloomDefaultBitsPerExpectedItem = 9 // bloom bit count = #expectedItems *  bloomDefaultBitsPerExpectedItem
	bloomDefaultHashes              = 6
	// default false positive rate (FPR) = pow(1 - exp(-bloomDefaultHashes/bloomDefaultBitsPerExpectedItem), bloomDefaultHashes) ~= 0.013

	bloomHdrMagic uint32 = 0x12341234
	bloomVersion  uint32 = 1
)

func calcBloomParams(expectedItems int) (bits, hashes int) {
	// complete formulas: (assume n = expectedItems, p = bloomFalsePositiveRate, m = bits, k = optimal number of hash functions)
	// m = -n*ln(p) / (ln(2)^2;
	// k = m/n * ln(2)
	// https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
	return expectedItems * bloomDefaultBitsPerExpectedItem, bloomDefaultHashes
}

func newCtreeBloom(bits, hashes int) *ctreeBloom {
	words := (bits + bloomWordBits - 1) >> bloomWordShift
	return &ctreeBloom{
		bitset:    make([]bloomWord, words),
		numHashes: hashes,
	}
}

func newCtreeBloomDefault(expectedItems int) *ctreeBloom {
	if expectedItems <= 0 || expectedItems > bloomSupportedMaxCtreeCount {
		return nil
	}
	return newCtreeBloom(calcBloomParams(expectedItems))
}

func bloomKey(key EntryKey) []byte {
	buf := NewSerializeBuf(key.Size())
	key.Encode(buf)
	return buf.Bytes()
}

func (cbloom *ctreeBloom) insert(key EntryKey) {
	if cbloom == nil {
		return
	}
	h, h2 := murmur3.Sum128(bloomKey(key))
	// other hashes are derived from this 2.
	// Proof that method doesn`t corrupt FPR: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf (chapter 3)
	n := uint64(cbloom.bits())
	for i := 0; i < cbloom.numHashes; i++ {
		h += h2
		at := h % n
		cbloom.bitset[at>>bloomWordShift] |= 1 << (at & (bloomWordBits - 1))
	}
}

func (cbloom *ctreeBloom) exists(key EntryKey) bool {
	if cbloom == nil {
		return true
	}
	h, h2 := murmur3.Sum128(bloomKey(key))
	// other hashes are derived from this 2.
	// Proof that method doesn`t corrupt FPR: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf (chapter 3)
	n := uint64(cbloom.bits())
	for i := 0; i < cbloom.numHashes; i++ {
		h += h2
		at := h % n
		if cbloom.bitset[at>>bloomWordShift]&(1<<(at&(bloomWordBits-1))) == 0 {
			return false
		}
	}
	return true
}

func (cbloom *ctreeBloom) bits() int {
	return len(cbloom.bitset) * bloomWordBits
}

// length return number of bloom bytes
func (cbloom *ctreeBloom) length() int {
	return cbloom.bits() / 8
}

func (cbloom *ctreeBloom) encode(buf *SerializeBuf, pageAlign bool) {
	checksumOffs := buf.Len()
	buf.EncodeFixedUint32(bloomHdrMagic)
	crcOffs := buf.Len()
	buf.EncodeFixedUint32(0) // placeholder for crc

	buf.EncodeUint64(uint64(cbloom.numHashes))
	buf.EncodeUint64(uint64(len(cbloom.bitset)))

	for _, item := range cbloom.bitset {
		buf.EncodeFixedUint64(uint64(item))
	}

	if pageAlign {
		// pad to page boundary
		buf.AlignTo(io.PageSize)
	}

	// calculate checksum
	crc := crc32.Checksum(buf.Bytes()[checksumOffs:], crcTab)
	binary.BigEndian.PutUint32(buf.Bytes()[crcOffs:], crc)
}

func decodeCtreeBloom(buf *DeserializeBuf) (cbloom *ctreeBloom, err error) {
	fullContent := buf.Bytes()
	magic := buf.DecodeFixedUint32()
	if magic != bloomHdrMagic {
		return nil, FirstErr(buf.Error(), fmt.Errorf("bloom magic mismatch %x != %x", bloomHdrMagic, magic))
	}

	crcBuf := buf.Bytes()
	expectedCrc := buf.DecodeFixedUint32()
	binary.BigEndian.PutUint32(crcBuf, 0)
	crc := crc32.Checksum(fullContent, crcTab)
	binary.BigEndian.PutUint32(crcBuf, crc)
	if expectedCrc != crc {
		return nil, FirstErr(buf.Error(), fmt.Errorf("bloom checksum mismatch %x %x", expectedCrc, crc))
	}

	hashes := int(buf.DecodeUint64())
	l := int(buf.DecodeUint64())
	bitset := make([]bloomWord, l)
	for i := 0; i < l; i++ {
		bitset[i] = bloomWord(buf.DecodeFixedUint64())
	}

	return &ctreeBloom{bitset, hashes}, nil
}
