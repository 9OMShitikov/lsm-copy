package lsm

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// SerializeBuf provides API to serialize binary data
type SerializeBuf struct {
	buf []byte
}

// DeserializeBuf provides an easy way to deseralize data
type DeserializeBuf struct {
	Ver uint32 // version of the content to decode, can be set before decoding for higher levels
	buf []byte
	err error
}

// --------------------------------------------------------

// NewSerializeBuf returns new buffer for serialization of data
func NewSerializeBuf(n int) *SerializeBuf {
	return &SerializeBuf{
		buf: make([]byte, 0, n),
	}
}

// Bytes returns current serialized bytes
func (sb *SerializeBuf) Bytes() []byte {
	return sb.buf
}

// Len returns current length of buffer collected
func (sb *SerializeBuf) Len() int {
	return len(sb.buf)
}

// WriteRaw appends raw data to buf w/o encoding
func (sb *SerializeBuf) WriteRaw(b []byte) {
	sb.buf = append(sb.buf, b...)
}

// EncodeSubSection writes an already encoded sub section along with its size
func (sb *SerializeBuf) EncodeSubSection(subsb *SerializeBuf) {
	sb.EncodeFixedUint32(uint32(subsb.Len()))
	sb.WriteRaw(subsb.buf)
}

// AlignTo aligns content to given boundary by zero padding
func (sb *SerializeBuf) AlignTo(x int) {
	tail := len(sb.buf) % x
	if tail == 0 {
		return
	}
	sb.buf = append(sb.buf, make([]byte, x-tail)...)
}

// EncodeBuf encodes given bytes buffer
func (sb *SerializeBuf) EncodeBuf(b []byte) {
	s := *(*string)(unsafe.Pointer(&b))
	sb.EncodeStr(s)
}

// EncodeStr encodes given bytes string
func (sb *SerializeBuf) EncodeStr(s string) {
	var tmp [16]byte
	slen := len(s)
	len := binary.PutUvarint(tmp[:], uint64(slen))
	sb.buf = append(sb.buf, tmp[:len]...)
	sb.buf = append(sb.buf, s...)
}

// EncodeInt64 encodes given int64
func (sb *SerializeBuf) EncodeInt64(x int64) {
	var tmp [16]byte
	len := binary.PutVarint(tmp[:], x)
	sb.buf = append(sb.buf, tmp[:len]...)
}

// EncodeUint64 encodes given uint64
func (sb *SerializeBuf) EncodeUint64(x uint64) {
	var tmp [16]byte
	len := binary.PutUvarint(tmp[:], x)
	sb.buf = append(sb.buf, tmp[:len]...)
}

// EncodeUint32 encodes given uint32
func (sb *SerializeBuf) EncodeUint32(x uint32) {
	sb.EncodeUint64(uint64(x))
}

// EncodeFixedUint64 encodes given uint64 with fixed size (8 byte)
func (sb *SerializeBuf) EncodeFixedUint64(x uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], x)
	sb.buf = append(sb.buf, tmp[:]...)
}

// EncodeFixedInt64 encodes given int64 with fixed size (8 byte)
func (sb *SerializeBuf) EncodeFixedInt64(x int64) {
	sb.EncodeFixedUint64(uint64(x))
}

// EncodeFixedUint32 encodes given uint32 with fixed size (4 byte)
func (sb *SerializeBuf) EncodeFixedUint32(x uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], x)
	sb.buf = append(sb.buf, tmp[:]...)
}

// EncodeFixedUint16 encodes given uint16 with fixed size (2 bytes)
func (sb *SerializeBuf) EncodeFixedUint16(x uint16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], x)
	sb.buf = append(sb.buf, tmp[:]...)
}

// EncodeFixedUint8 encodes given uint8 with fixed size (1 byte)
func (sb *SerializeBuf) EncodeFixedUint8(x uint8) {
	sb.buf = append(sb.buf, x)
}

// EncodeBool encodes given bool
func (sb *SerializeBuf) EncodeBool(x bool) {
	var b byte
	if x {
		b = 1
	}
	sb.buf = append(sb.buf, b)
}

// --------------------------------------------------------

// NewDeserializeBuf returns new buffer to deserialize
func NewDeserializeBuf(buf []byte) *DeserializeBuf {
	return &DeserializeBuf{
		buf: buf,
	}
}

// SetError sets a string error message to current state (if not yet)
func (db *DeserializeBuf) SetError(s string) {
	if db.err != nil {
		return
	}

	db.err = errors.New(s)
}

// Error returns first deserialization error encountered
func (db *DeserializeBuf) Error() error {
	return db.err
}

// Len returns bytes left to deserialize
func (db *DeserializeBuf) Len() int {
	return len(db.buf)
}

// Bytes returns bytes left to deserialize
func (db *DeserializeBuf) Bytes() []byte {
	return db.buf
}

// DecodeSubSection returns a sub-section to decode
func (db *DeserializeBuf) DecodeSubSection() *DeserializeBuf {
	l := db.DecodeFixedUint32()
	if len(db.buf) < int(l) {
		db.SetError("Not enough bytes in sub section to decode")
		return &DeserializeBuf{}
	}
	res := &DeserializeBuf{buf: db.buf[:l]}
	db.buf = db.buf[l:]
	return res
}

// DecodeBuf extracts buffer from the encoded buffer
// NOTE: returned value holds a reference to the whole buffer! If need to hold long lived reference, need to clone it
func (db *DeserializeBuf) DecodeBuf() []byte {
	if db.err != nil {
		return nil
	}

	val, vlen := binary.Uvarint(db.buf)
	if vlen <= 0 || val > maxEntrySize {
		db.SetError("Can't decode uint32 in DecodeBuf")
		return nil
	}

	total := vlen + int(val)
	if len(db.buf) < total {
		db.SetError("Can't decode buf/string - not enough bytes")
		return nil
	}
	b := db.buf[vlen:total]
	db.buf = db.buf[total:]
	return b
}

// DecodeStr extracts string from the encoded buffer
// NOTE: 's' holds a reference to the whole buffer! If need to hold long lived reference, use DecodeStrCopy() instead.
func (db *DeserializeBuf) DecodeStr() string {
	b := db.DecodeBuf()
	if db.err != nil || b == nil {
		return ""
	}
	s := *(*string)(unsafe.Pointer(&b))
	return s
}

// DecodeStrCopy extracts string from the encoded buffer by allocating a new string with content
func (db *DeserializeBuf) DecodeStrCopy() string {
	b := db.DecodeBuf()
	b2 := make([]byte, len(b))
	copy(b2, b)
	return *(*string)(unsafe.Pointer(&b2))
}

// DecodeInt64 decodes int64
func (db *DeserializeBuf) DecodeInt64() int64 {
	if db.err != nil {
		return 0
	}
	val, vlen := binary.Varint(db.buf)
	if vlen <= 0 {
		db.SetError("Can't decode int64")
		return 0
	}
	db.buf = db.buf[vlen:]
	return val
}

// DecodeUint64 decodes uint64
func (db *DeserializeBuf) DecodeUint64() uint64 {
	if db.err != nil {
		return 0
	}
	val, vlen := binary.Uvarint(db.buf)
	if vlen <= 0 {
		db.SetError("Can't decode uint64")
		return 0
	}
	db.buf = db.buf[vlen:]
	return val
}

// DecodeUint32 decodes uint32
func (db *DeserializeBuf) DecodeUint32() uint32 {
	x := db.DecodeUint64()
	if x < (1 << 32) {
		return uint32(x)
	}
	db.SetError("Can't decode uint32 - value too large")
	return 0
}

// DecodeFixedUint64 decodes uint64, that was encoded with fixed len
func (db *DeserializeBuf) DecodeFixedUint64() uint64 {
	if db.err != nil {
		return 0
	}
	if len(db.buf) < 8 {
		db.SetError("Can't decode fixed-uint64")
		return 0
	}
	val := binary.BigEndian.Uint64(db.buf)
	db.buf = db.buf[8:]
	return val
}

// DecodeFixedInt64 decodes int64, that was encoded with fixed len
func (db *DeserializeBuf) DecodeFixedInt64() int64 {
	return int64(db.DecodeFixedUint64())
}

// DecodeFixedUint32 decodes uint32, that was encoded with fixed len
func (db *DeserializeBuf) DecodeFixedUint32() uint32 {
	if db.err != nil {
		return 0
	}
	if len(db.buf) < 4 {
		db.SetError("Can't decode fixed-uint32")
		return 0
	}
	val := binary.BigEndian.Uint32(db.buf)
	db.buf = db.buf[4:]
	return val
}

// DecodeFixedUint16 decodes uint16, that was encoded with fixed len
func (db *DeserializeBuf) DecodeFixedUint16() uint16 {
	if db.err != nil {
		return 0
	}
	if len(db.buf) < 2 {
		db.SetError("Can't decode fixed-uint16")
		return 0
	}
	val := binary.BigEndian.Uint16(db.buf)
	db.buf = db.buf[2:]
	return val
}

// DecodeFixedUint8 decodes uint8, that was encoded with fixed len
func (db *DeserializeBuf) DecodeFixedUint8() uint8 {
	if db.err != nil {
		return 0
	}
	if len(db.buf) < 1 {
		db.SetError("Can't decode fixed-uint8")
		return 0
	}
	val := db.buf[0]
	db.buf = db.buf[1:]
	return val
}

// DecodeBool decodes bool
func (db *DeserializeBuf) DecodeBool() (res bool) {
	val := db.DecodeFixedUint8()
	if val != 0 {
		res = true
	}
	return
}

// ReadRaw fetches given number of bytes to buffer
func (db *DeserializeBuf) ReadRaw(buf []byte) {
	if db.err != nil {
		return
	}
	if len(db.buf) < len(buf) {
		db.SetError("ReadRaw(): not enough bytes")
		return
	}
	copy(buf, db.buf)
	db.buf = db.buf[len(buf):]
}
