package storage

import (
	"encoding/binary"

	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

const (
	offsetDocBlockCodec     = 0  // 1 byte  (C) Codec
	offsetDocBlockLength    = 1  // 8 bytes (L) Length
	offsetDocBlockRawLength = 9  // 8 bytes (U) Raw length (after decompression)
	offsetDocBlockExt1      = 17 // 8 bytes (E) Extensions/flags
	offsetDocBlockExt2      = 25 // 8 bytes (E) Extensions/flags

	DocBlockHeaderLen = 33
)

// DocBlock format: C : LLLL-LLLL : UUUU-UUUU : EEEE-EEEE : EEEE-EEEE
// See: /docs/format-docs-meta-file.md

type DocBlock []byte

func NewBlock() DocBlock {
	return make(DocBlock, DocBlockHeaderLen)
}

func (b DocBlock) Codec() Codec {
	return Codec(b[offsetDocBlockCodec])
}

func (b DocBlock) SetCodec(codecVal Codec) {
	b[offsetDocBlockCodec] = byte(codecVal)
}

func (b DocBlock) Len() uint64 {
	return binary.LittleEndian.Uint64(b[offsetDocBlockLength:])
}

func (b DocBlock) SetLen(val uint64) {
	binary.LittleEndian.PutUint64(b[offsetDocBlockLength:], val)
}

func (b DocBlock) FullLen() uint64 {
	return b.Len() + DocBlockHeaderLen
}

func (b DocBlock) CalcLen() {
	b.SetLen(uint64(len(b) - DocBlockHeaderLen))
}

func (b DocBlock) RawLen() uint64 {
	return binary.LittleEndian.Uint64(b[offsetDocBlockRawLength:])
}

func (b DocBlock) SetRawLen(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetDocBlockRawLength:], x)
}

func (b DocBlock) GetExt1() uint64 {
	return binary.LittleEndian.Uint64(b[offsetDocBlockExt1:])
}
func (b DocBlock) SetExt1(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetDocBlockExt1:], x)
}

func (b DocBlock) GetExt2() uint64 {
	return binary.LittleEndian.Uint64(b[offsetDocBlockExt2:])
}
func (b DocBlock) SetExt2(x uint64) {
	binary.LittleEndian.PutUint64(b[offsetDocBlockExt2:], x)
}

func (b DocBlock) Payload() []byte {
	return b[DocBlockHeaderLen:]
}

func CompressDocBlock(src []byte, dst DocBlock, zstdLevel int) DocBlock {
	dst = append(dst[:0], make([]byte, DocBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = zstd.CompressLevel(src, dst, zstdLevel)

	dst.CalcLen()
	dst.SetRawLen(uint64(len(src)))
	dst.SetCodec(CodecZSTD)

	return dst
}

func PackDocBlock(payload []byte, dst DocBlock) DocBlock {
	dst = append(dst[:0], make([]byte, DocBlockHeaderLen)...) // fill header with zeros for cleanup
	dst = append(dst, payload...)

	dst.CalcLen()
	dst.SetRawLen(uint64(len(payload)))
	dst.SetCodec(CodecNo)

	return dst
}

// DecompressTo always put the result in `dst` regardless of whether unpacking is required
// or part of the DocBlock can be enough.
//
// So DocBlock does not share the same data with `dst` and can be used safely
func (b DocBlock) DecompressTo(dst []byte) ([]byte, error) {
	payload := b.Payload()
	if b.Codec() == CodecNo {
		dst = util.EnsureSliceSize(dst, len(payload))
		copy(dst, payload)
		return dst, nil
	}
	return b.Codec().decompressBlock(int(b.RawLen()), payload, dst)
}
