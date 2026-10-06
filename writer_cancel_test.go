package lz4_test

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/pierrec/lz4/v4/internal/lz4block"
)

// gateWriter blocks the first block write (the write right after the frame
// header) until released, so that a compressed block stays stuck in the
// write pipeline. It records everything written to it.
type gateWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	writes  int
	entered chan struct{} // closed when the blocking write is entered
	release chan struct{} // closed to unblock it
	once    sync.Once
}

func (g *gateWriter) Write(p []byte) (int, error) {
	g.mu.Lock()
	g.writes++
	blocking := g.writes == 2 // the first write is the frame header
	g.mu.Unlock()
	if blocking {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *gateWriter) bytes() []byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]byte(nil), g.buf.Bytes()...)
}

// TestWriterCancelStuckBlock cancels a concurrent Writer while a compressed
// block is stuck in the write pipeline. It verifies that:
//   - no new blocks are accepted after the cancellation,
//   - late compression results are dropped, not written out,
//   - the committed prefix is explicitly incomplete (no end mark),
//   - every buffer borrowed from the shared pools is returned exactly once,
//   - the shared pools are not affected: another writer works normally.
func TestWriterCancelStuckBlock(t *testing.T) {
	const nblocks = 3
	payload := make([]byte, nblocks*int(lz4.Block64Kb))
	for i := range payload {
		payload[i] = byte(i * 31)
	}

	g := &gateWriter{entered: make(chan struct{}), release: make(chan struct{})}
	zw := lz4.NewWriter(g)
	if err := zw.Apply(lz4.BlockSizeOption(lz4.Block64Kb), lz4.ConcurrencyOption(4)); err != nil {
		t.Fatal(err)
	}

	gets0, puts0 := lz4block.GetCount(), lz4block.PutCount()

	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}

	// Barrier 1: the first compressed block is stuck in the pipeline and
	// the remaining ones are queued behind it.
	<-g.entered

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- zw.Cancel() }()

	// Barrier 2: wait until Cancel has stopped the pipeline and returned
	// the pending buffer to the pool. Cancel stops the pipeline before
	// returning that buffer, and the pool counters are atomic, so this
	// observes the cancellation without racing the Writer.
	for lz4block.PutCount() == puts0 {
		runtime.Gosched()
	}
	// Releasing the stuck block now turns its result, and all the queued
	// ones, into late results: they must not be written out.
	close(g.release)

	if err := <-cancelDone; !errors.Is(err, lz4.ErrWriterCanceled) {
		t.Fatalf("Cancel() = %v, want ErrWriterCanceled", err)
	}

	// No new blocks are accepted after the cancellation.
	if _, err := zw.Write(payload[:1]); !errors.Is(err, lz4.ErrWriterCanceled) {
		t.Fatalf("Write after Cancel = %v, want ErrWriterCanceled", err)
	}

	// The committed prefix is explicitly incomplete: it holds the frame
	// header and the first block only, with no end mark, so a reader must
	// report an error instead of a valid complete frame.
	out := g.bytes()
	zr := lz4.NewReader(bytes.NewReader(out))
	got, err := io.ReadAll(zr)
	if err == nil {
		t.Fatal("canceled stream read as a complete frame")
	}
	if len(got) != int(lz4.Block64Kb) {
		t.Fatalf("canceled stream yielded %d bytes, want only the first %d block bytes", len(got), lz4.Block64Kb)
	}
	// Return the buffers held by the failed reader to the pool.
	zr.Reset(bytes.NewReader(nil))

	// The shared pools are not affected by the canceled session: another
	// writer using the same pools works normally.
	var buf2 bytes.Buffer
	zw2 := lz4.NewWriter(&buf2)
	if _, err := zw2.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw2.Close(); err != nil {
		t.Fatal(err)
	}
	got2, err := io.ReadAll(lz4.NewReader(&buf2))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, payload) {
		t.Fatal("round trip through a second writer failed")
	}

	// The canceled writer itself is reusable after a Reset, and Close
	// keeps its usual behavior of writing a complete frame.
	var buf3 bytes.Buffer
	zw.Reset(&buf3)
	if _, err := zw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	got3, err := io.ReadAll(lz4.NewReader(&buf3))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got3, payload) {
		t.Fatal("round trip through the reset writer failed")
	}

	// Every buffer borrowed from the shared pools during this test has
	// been returned exactly once.
	if gets, puts := lz4block.GetCount()-gets0, lz4block.PutCount()-puts0; gets != puts {
		t.Fatalf("pool buffers leaked: borrowed %d, returned %d", gets, puts)
	}
}

// TestWriterCancelNonConcurrent checks the cancellation of a writer running
// with a concurrency level of 1: buffered data is discarded, the stream is
// left incomplete, and no new blocks are accepted.
func TestWriterCancelNonConcurrent(t *testing.T) {
	gets0, puts0 := lz4block.GetCount(), lz4block.PutCount()

	var buf bytes.Buffer
	zw := lz4.NewWriter(&buf) // default concurrency is 1
	if _, err := zw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Cancel(); !errors.Is(err, lz4.ErrWriterCanceled) {
		t.Fatalf("Cancel() = %v, want ErrWriterCanceled", err)
	}
	if _, err := zw.Write([]byte("x")); !errors.Is(err, lz4.ErrWriterCanceled) {
		t.Fatalf("Write after Cancel = %v, want ErrWriterCanceled", err)
	}

	// Only the frame header was written: the pending data was discarded
	// and no end mark was added, so the stream is not a valid frame.
	zr := lz4.NewReader(bytes.NewReader(buf.Bytes()))
	if _, err := io.ReadAll(zr); err == nil {
		t.Fatal("canceled stream read as a complete frame")
	}
	// Return the buffers held by the failed reader to the pool.
	zr.Reset(bytes.NewReader(nil))

	if gets, puts := lz4block.GetCount()-gets0, lz4block.PutCount()-puts0; gets != puts {
		t.Fatalf("pool buffers leaked: borrowed %d, returned %d", gets, puts)
	}
}

// failWriter fails writes with err after allowing failAfter successful ones.
type failWriter struct {
	failAfter int
	err       error
	n         int
	onFail    chan struct{} // closed when the first failure happens
	once      sync.Once
}

func (f *failWriter) Write(p []byte) (int, error) {
	f.n++
	if f.n > f.failAfter {
		if f.onFail != nil {
			f.once.Do(func() { close(f.onFail) })
		}
		return 0, f.err
	}
	return len(p), nil
}

// TestWriterCancelKeepsOriginalError checks that Cancel does not mask an
// error the session had already hit: the original cause is preserved.
func TestWriterCancelKeepsOriginalError(t *testing.T) {
	errBoom := errors.New("boom")

	// Non-concurrent writer: the write error is reported synchronously and
	// must survive the cancellation.
	fw := &failWriter{failAfter: 1, err: errBoom} // allow the frame header only
	zw := lz4.NewWriter(fw)
	if err := zw.Apply(lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(make([]byte, int(lz4.Block64Kb))); !errors.Is(err, errBoom) {
		t.Fatalf("Write = %v, want %v", err, errBoom)
	}
	if err := zw.Cancel(); !errors.Is(err, errBoom) {
		t.Fatalf("Cancel = %v, want original cause %v", err, errBoom)
	}

	// Concurrent writer: the block fails in the pipeline; Cancel must keep
	// the original cause alongside ErrWriterCanceled.
	fw2 := &failWriter{failAfter: 1, err: errBoom, onFail: make(chan struct{})}
	zw2 := lz4.NewWriter(fw2)
	if err := zw2.Apply(lz4.BlockSizeOption(lz4.Block64Kb), lz4.ConcurrencyOption(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := zw2.Write(make([]byte, 2*int(lz4.Block64Kb))); err != nil {
		t.Fatal(err)
	}
	// Barrier: wait until the first block write has actually failed in the
	// pipeline, so its error is the one Cancel must preserve.
	<-fw2.onFail
	if err := zw2.Cancel(); !errors.Is(err, errBoom) || !errors.Is(err, lz4.ErrWriterCanceled) {
		t.Fatalf("Cancel = %v, want ErrWriterCanceled wrapping %v", err, errBoom)
	}
}
