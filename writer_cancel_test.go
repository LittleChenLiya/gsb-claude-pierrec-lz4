package lz4_test

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
)

// This file exercises Writer.Cancel, the explicit cancel session of the
// parallel Writer.
//
// Resource lifecycle of a canceled session (see Writer.Cancel):
//   - every block compression routine owns the buffers it borrowed from the
//     shared lz4block pool (one source buffer, one block buffer) and returns
//     each of them exactly once, whether its result is written out or
//     discarded;
//   - the pending accumulation buffer stays with the Writer and is returned
//     by Close or Reset as usual;
//   - the shared pool is only ever returned to, never closed, so other
//     writers keep borrowing from it normally;
//   - the bytes committed to the destination before the cancellation are
//     left as an incomplete frame: no end mark and no content checksum are
//     appended, so readers reject the prefix instead of accepting it as a
//     valid complete frame.

// blockHandlerGate is a test barrier installed through OnBlockDoneOption:
// every block report blocks until Release is called, which stalls the block
// compression routines after they have borrowed their buffers and compressed
// their block, but before they deliver the result.
type blockHandlerGate struct {
	release chan struct{} // closed to release all blocked reports
	all     chan struct{} // closed once want reports are blocked
	want    int32
	entered atomic.Int32
	on      atomic.Bool
}

func newBlockHandlerGate(want int) *blockHandlerGate {
	return &blockHandlerGate{
		release: make(chan struct{}),
		all:     make(chan struct{}),
		want:    int32(want),
	}
}

func (g *blockHandlerGate) handler(int) {
	if g.entered.Add(1) == g.want {
		close(g.all)
	}
	<-g.release
}

// Release unblocks all stalled reports. It is idempotent.
func (g *blockHandlerGate) Release() {
	if g.on.CompareAndSwap(false, true) {
		close(g.release)
	}
}

// gatedWriter is a test barrier for the destination: writes pass through
// until the blockAt-th Write call, which blocks until Release is called.
type gatedWriter struct {
	buf     bytes.Buffer
	calls   atomic.Int32
	blockAt int32
	entered chan struct{} // closed when the gated write is entered
	release chan struct{} // closed to let the gated write through
	on      atomic.Bool
}

func newGatedWriter(blockAt int) *gatedWriter {
	return &gatedWriter{
		blockAt: int32(blockAt),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *gatedWriter) Write(p []byte) (int, error) {
	if w.calls.Add(1) == w.blockAt {
		close(w.entered)
		<-w.release
	}
	return w.buf.Write(p)
}

// Release unblocks the gated write. It is idempotent.
func (w *gatedWriter) Release() {
	if w.on.CompareAndSwap(false, true) {
		close(w.release)
	}
}

// failAfterWriter allows the first allow bytes and then fails every write
// with err, keeping the original cause for the session to report. The
// failed channel is closed when the first failure happens; it is a barrier
// letting the test wait until the block collector has recorded the error.
// Only the collector goroutine calls Write, so no synchronization is needed.
type failAfterWriter struct {
	allow  int
	err    error
	failed chan struct{} // closed on the first failure, never written after
	on     atomic.Bool
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.allow < len(p) {
		if w.failed != nil && w.on.CompareAndSwap(false, true) {
			close(w.failed)
		}
		return 0, w.err
	}
	w.allow -= len(p)
	return len(p), nil
}

func cancelTestBlock(b byte) []byte {
	return bytes.Repeat([]byte{b}, 64<<10)
}

// TestWriterCancelStuckBlocks cancels a parallel writer while its block
// compression routines are stuck after compression: once released, their
// results must not be written out, every borrowed buffer must be returned
// exactly once, and another writer must keep working normally.
func TestWriterCancelStuckBlocks(t *testing.T) {
	lz4block.PoolAccountingSet(true)
	lz4block.PoolAccountingReset()
	defer lz4block.PoolAccountingSet(false)

	var out bytes.Buffer
	// With concurrency 2 the pipeline holds three block routines: one being
	// collected, two queued. The barrier stalls all three of them.
	gate := newBlockHandlerGate(3)
	defer gate.Release()

	zw := lz4.NewWriter(&out)
	if err := zw.Apply(
		lz4.ConcurrencyOption(2),
		lz4.BlockSizeOption(lz4.Block64Kb),
		lz4.OnBlockDoneOption(gate.handler),
	); err != nil {
		t.Fatal(err)
	}

	// The fourth Write fills its block and then blocks on the full queue.
	writeErr := make(chan error, 1)
	go func() {
		var err error
		for i := byte(0); i < 4 && err == nil; i++ {
			_, err = zw.Write(cancelTestBlock(i))
		}
		writeErr <- err
	}()

	// Barrier: all three block routines are stuck after compression,
	// holding their buffers, and the block queue is full.
	<-gate.all

	cancelErr := make(chan error, 1)
	go func() { cancelErr <- zw.Cancel() }()

	// The canceled session accepts no new block: the stuck Write is
	// unblocked with the cancel error, and so are later calls.
	if err := <-writeErr; !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("blocked Write: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}
	if _, err := zw.Write(nil); !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Write after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}
	if err := zw.Flush(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Flush after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}

	// Release the stuck compression results: none of them may be written.
	gate.Release()

	if err := <-cancelErr; !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}

	// The committed prefix is only the frame header (magic, flags, header
	// checksum): every block result arrived after the cancellation and was
	// discarded, and no end mark was appended.
	if got := out.Len(); got != 7 {
		t.Fatalf("committed prefix: got %d bytes, want the 7 byte frame header", got)
	}

	// Every buffer borrowed by the stuck block routines has been returned;
	// only the writer's pending accumulation buffer is still checked out.
	if borrows, returns := lz4block.PoolAccounting(); borrows-returns != 1 {
		t.Fatalf("pool after Cancel: borrows=%d returns=%d, want exactly one buffer outstanding", borrows, returns)
	}

	// Close reports the cancellation and releases the pending buffer.
	if err := zw.Close(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Close after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}
	if borrows, returns := lz4block.PoolAccounting(); borrows != returns {
		t.Fatalf("pool after Close: borrows=%d returns=%d, want equal counts", borrows, returns)
	}

	// The prefix is visibly incomplete: readers reject it instead of
	// mistaking it for a valid complete frame.
	zr := lz4.NewReader(bytes.NewReader(out.Bytes()))
	if _, err := zr.Read(make([]byte, 1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("reading canceled stream: got %v, want %v", err, io.ErrUnexpectedEOF)
	}

	// The shared pool was only returned to, never closed: another writer
	// borrows from it and works normally.
	lz4block.PoolAccountingReset()
	payload := []byte("another writer keeps compressing after the cancel session")
	var buf bytes.Buffer
	zw2 := lz4.NewWriter(&buf)
	if _, err := zw2.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw2.Close(); err != nil {
		t.Fatal(err)
	}
	if borrows, returns := lz4block.PoolAccounting(); borrows != returns {
		t.Fatalf("pool after second writer: borrows=%d returns=%d, want equal counts", borrows, returns)
	}
	got, err := io.ReadAll(lz4.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("second writer roundtrip: got %q, want %q", got, payload)
	}
}

// TestWriterCancelCommittedPrefix cancels while a destination write is in
// flight: the blocks committed before the cancellation stay, the late
// results are discarded, and the prefix is a readable but incomplete frame.
func TestWriterCancelCommittedPrefix(t *testing.T) {
	lz4block.PoolAccountingSet(true)
	lz4block.PoolAccountingReset()
	defer lz4block.PoolAccountingSet(false)

	// Destination write calls: 1=header, 2..3=block 0, 4..5=block 1.
	// The barrier blocks call 4, with block 1 in flight and the queue full.
	gw := newGatedWriter(4)
	defer gw.Release()

	zw := lz4.NewWriter(gw)
	if err := zw.Apply(lz4.ConcurrencyOption(2), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
		t.Fatal(err)
	}

	// Five blocks: two committed or in flight, two queued, one stuck.
	writeErr := make(chan error, 1)
	go func() {
		var err error
		for i := byte(0); i < 5 && err == nil; i++ {
			_, err = zw.Write(cancelTestBlock(i))
		}
		writeErr <- err
	}()

	// Barrier: the collector is inside the destination write of block 1,
	// so block 0 is committed and block 1 is being written.
	<-gw.entered

	cancelErr := make(chan error, 1)
	go func() { cancelErr <- zw.Cancel() }()

	// The stuck Write is unblocked by the cancellation. Observing the
	// cancel error here guarantees that the collector has been flagged:
	// Cancel marks the collector before closing the cancel channel.
	if err := <-writeErr; !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("blocked Write: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}

	// Let the in-flight destination write complete.
	gw.Release()

	if err := <-cancelErr; !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}
	if err := zw.Close(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Close after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}

	// All buffers borrowed by the block routines and the writer itself
	// have been returned to the shared pool.
	if borrows, returns := lz4block.PoolAccounting(); borrows != returns {
		t.Fatalf("pool after cancel session: borrows=%d returns=%d, want equal counts", borrows, returns)
	}

	// The prefix holds exactly blocks 0 and 1: the in-flight block was
	// committed, the queued results were discarded, and no end mark nor
	// content checksum makes it look like a complete frame.
	want := append(cancelTestBlock(0), cancelTestBlock(1)...)
	got, err := io.ReadAll(lz4.NewReader(bytes.NewReader(gw.buf.Bytes())))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("reading canceled stream: got err=%v, want %v", err, io.ErrUnexpectedEOF)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("committed prefix: got %d bytes of data, want %d (blocks 0 and 1)", len(got), len(want))
	}
}

// TestWriterCancelKeepsCause checks that the cancel error preserves the
// original cause of the session failure.
func TestWriterCancelKeepsCause(t *testing.T) {
	errBoom := errors.New("boom")
	// Let the frame header through, then fail every destination write.
	fw := &failAfterWriter{allow: 7, err: errBoom, failed: make(chan struct{})}

	zw := lz4.NewWriter(fw)
	if err := zw.Apply(lz4.ConcurrencyOption(2), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(cancelTestBlock(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(cancelTestBlock(2)); err != nil {
		t.Fatal(err)
	}

	// Barrier: the collector has recorded the destination failure.
	<-fw.failed

	err := zw.Cancel()
	if !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Cancel: got %v, want the original cause %v preserved", err, errBoom)
	}
	// Close reports the same cause.
	if err := zw.Close(); !errors.Is(err, errBoom) || !errors.Is(err, lz4errors.ErrWriterCanceled) {
		t.Fatalf("Close after cancel: got %v, want %v wrapping %v", err, lz4errors.ErrWriterCanceled, errBoom)
	}
}

// TestWriterCancelSession covers cancellation outside of backpressure:
// before any write, twice, around Close and Reset, after a normal Close,
// and with a sequential (concurrency 1) writer.
func TestWriterCancelSession(t *testing.T) {
	t.Run("BeforeAnyWrite", func(t *testing.T) {
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf)
		if err := zw.Apply(lz4.ConcurrencyOption(4)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Cancel(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		// Cancel is idempotent.
		if err := zw.Cancel(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("second Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		if _, err := zw.Write([]byte("x")); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Write after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		if err := zw.Close(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Close after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		if buf.Len() != 0 {
			t.Fatalf("destination got %d bytes, want none", buf.Len())
		}

		// Reset starts a fresh session: the writer works normally again.
		payload := []byte("reused after cancel")
		zw.Reset(&buf)
		if _, err := zw.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(lz4.NewReader(&buf))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("roundtrip after Reset: got %q, want %q", got, payload)
		}
		// Cancel after a normal Close is a no-op.
		if err := zw.Cancel(); err != nil {
			t.Fatalf("Cancel after Close: got %v, want nil", err)
		}
	})

	t.Run("Sequential", func(t *testing.T) {
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf) // default concurrency is 1
		if err := zw.Apply(lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write(cancelTestBlock(1)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Cancel(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		if _, err := zw.Write(cancelTestBlock(2)); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Write after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		if err := zw.Close(); !errors.Is(err, lz4errors.ErrWriterCanceled) {
			t.Fatalf("Close after cancel: got %v, want %v", err, lz4errors.ErrWriterCanceled)
		}
		// The committed block is readable, but the frame is incomplete.
		got, err := io.ReadAll(lz4.NewReader(bytes.NewReader(buf.Bytes())))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("reading canceled stream: got err=%v, want %v", err, io.ErrUnexpectedEOF)
		}
		if !bytes.Equal(got, cancelTestBlock(1)) {
			t.Fatalf("committed prefix: got %d bytes of data, want %d", len(got), len(cancelTestBlock(1)))
		}
	})
}
