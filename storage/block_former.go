// Package disk implements read write fraction routines
package storage

import (
	"time"
)

type BlockFormer struct {
	Buf            []byte
	writer         *BlocksWriter
	blockThreshold int

	// stats
	start time.Time
	stats BlockStats
}

type FlushOptions struct {
	ext1              uint64
	ext2              uint64
	zstdCompressLevel int
}

func NewDefaultFlushOptions() *FlushOptions {
	const zstdFastestLevel = -5
	return &FlushOptions{
		ext1:              0,
		ext2:              0,
		zstdCompressLevel: zstdFastestLevel,
	}
}

type FlushOption func(*FlushOptions)

func WithExt(ext1, ext2 uint64) FlushOption {
	return func(o *FlushOptions) {
		o.ext1 = ext1
		o.ext2 = ext2
	}
}

func WithZstdCompressLevel(level int) FlushOption {
	return func(o *FlushOptions) {
		o.zstdCompressLevel = level
	}
}

func NewBlockFormer(blockType string, writer *BlocksWriter, blockSize int, buf []byte) *BlockFormer {
	return &BlockFormer{
		Buf:            buf[:0],
		writer:         writer,
		blockThreshold: blockSize,
		start:          time.Now(),
		stats:          BlockStats{Name: blockType},
	}
}

func (b *BlockFormer) FlushIfNeeded(options ...FlushOption) (bool, error) {
	if len(b.Buf) > b.blockThreshold {
		return true, b.FlushForced(options...)
	}
	return false, nil
}

func (b *BlockFormer) FlushForced(options ...FlushOption) error {
	if len(b.Buf) == 0 {
		return nil
	}

	o := NewDefaultFlushOptions()
	for _, applyFn := range options {
		applyFn(o)
	}

	n, err := b.writer.WriteBlock(b.stats.Name, b.Buf, true, o.zstdCompressLevel, o.ext1, o.ext2)
	if err != nil {
		return err
	}

	b.stats.Blocks++
	b.stats.Raw += uint64(len(b.Buf))
	b.stats.Comp += uint64(n)

	b.Buf = b.Buf[:0]
	return nil
}

func (b *BlockFormer) GetStats() *BlockStats {
	b.stats.Duration = time.Since(b.start)
	return &b.stats
}
