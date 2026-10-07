// Package lz4block provides LZ4 BlockSize types and pools of buffers.
package lz4block

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	Block64Kb uint32 = 1 << (16 + iota*2)
	Block256Kb
	Block1Mb
	Block4Mb
	Block8Mb = 2 * Block4Mb

	// legacyBlockBound is CompressBlockBound(Block8Mb): legacy frames have
	// no uncompressed blocks, so a compressed block may be larger than 8MB.
	legacyBlockBound = Block8Mb + Block8Mb/255 + 16
)

var (
	blockPool64K  = sync.Pool{New: func() any { return &[Block64Kb]byte{} }}
	blockPool256K = sync.Pool{New: func() any { return &[Block256Kb]byte{} }}
	blockPool1M   = sync.Pool{New: func() any { return &[Block1Mb]byte{} }}
	blockPool4M   = sync.Pool{New: func() any { return &[Block4Mb]byte{} }}
	blockPool8M   = sync.Pool{New: func() any { return &[Block8Mb]byte{} }}

	blockPoolLegacy = sync.Pool{New: func() any { return &[legacyBlockBound]byte{} }}
)

// Pool accounting, for tests and diagnostics. It is disabled by default;
// enable it with PoolAccountingSet. While enabled, every buffer borrowed
// from one of the pools above increments the borrow counter and every
// buffer returned increments the return counter, so a balanced session
// ends with equal counts. The pools themselves are process-wide and are
// never closed: returning a buffer only makes it available to other users.
var (
	poolAccounting atomic.Bool
	poolBorrows    atomic.Int64
	poolReturns    atomic.Int64
)

// PoolAccountingSet enables or disables pool borrow/return accounting.
func PoolAccountingSet(on bool) { poolAccounting.Store(on) }

// PoolAccounting returns the number of buffers borrowed from and returned
// to the pools since the counters were last reset.
func PoolAccounting() (borrows, returns int64) {
	return poolBorrows.Load(), poolReturns.Load()
}

// PoolAccountingReset zeroes the pool borrow/return counters.
func PoolAccountingReset() {
	poolBorrows.Store(0)
	poolReturns.Store(0)
}

func accountGet() {
	if poolAccounting.Load() {
		poolBorrows.Add(1)
	}
}

func accountPut() {
	if poolAccounting.Load() {
		poolReturns.Add(1)
	}
}

func Index(b uint32) BlockSizeIndex {
	switch b {
	case Block64Kb:
		return 4
	case Block256Kb:
		return 5
	case Block1Mb:
		return 6
	case Block4Mb:
		return 7
	case Block8Mb: // only valid in legacy mode
		return 3
	}
	return 0
}

func IsValid(b uint32) bool {
	return Index(b) > 0
}

type BlockSizeIndex uint8

func (b BlockSizeIndex) IsValid() bool {
	switch b {
	case 4, 5, 6, 7:
		return true
	}
	return false
}

func (b BlockSizeIndex) Get() []byte {
	accountGet()
	switch b {
	case 4:
		return blockPool64K.Get().(*[Block64Kb]byte)[:]
	case 5:
		return blockPool256K.Get().(*[Block256Kb]byte)[:]
	case 6:
		return blockPool1M.Get().(*[Block1Mb]byte)[:]
	case 7:
		return blockPool4M.Get().(*[Block4Mb]byte)[:]
	case 3:
		return blockPool8M.Get().(*[Block8Mb]byte)[:]
	default:
		panic(fmt.Errorf("invalid block index %d", b))
	}
}

// GetLegacy returns a buffer for one compressed legacy block.
func GetLegacy() []byte {
	accountGet()
	return blockPoolLegacy.Get().(*[legacyBlockBound]byte)[:]
}

func Put(buf []byte) {
	// Safeguard: do not allow invalid buffers.
	switch c := cap(buf); uint32(c) {
	case Block64Kb:
		accountPut()
		blockPool64K.Put((*[Block64Kb]byte)(buf[:c]))
	case Block256Kb:
		accountPut()
		blockPool256K.Put((*[Block256Kb]byte)(buf[:c]))
	case Block1Mb:
		accountPut()
		blockPool1M.Put((*[Block1Mb]byte)(buf[:c]))
	case Block4Mb:
		accountPut()
		blockPool4M.Put((*[Block4Mb]byte)(buf[:c]))
	case Block8Mb:
		accountPut()
		blockPool8M.Put((*[Block8Mb]byte)(buf[:c]))
	case legacyBlockBound:
		accountPut()
		blockPoolLegacy.Put((*[legacyBlockBound]byte)(buf[:c]))
	case 0:
		// Allow "returning" an empty buffer.
	default:
		panic(fmt.Errorf("invalid block size %d", c))
	}
}

type CompressionLevel uint32

const (
	Fast CompressionLevel = 0
	// CCompatFast selects CompressorCCompat. The HC levels are powers of two from
	// 1<<9, so it cannot be mistaken for one.
	CCompatFast CompressionLevel = 1
)
