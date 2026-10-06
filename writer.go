package lz4

import (
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
	"github.com/pierrec/lz4/v4/internal/lz4stream"
)

var writerStates = []aState{
	noState:     newState,
	newState:    writeState,
	writeState:  closedState,
	closedState: newState,
	errorState:  newState,
}

// NewWriter returns a new LZ4 frame encoder.
func NewWriter(w io.Writer) *Writer {
	zw := &Writer{frame: lz4stream.NewFrame()}
	zw.state.init(writerStates)
	_ = zw.Apply(DefaultBlockSizeOption, DefaultChecksumOption, DefaultConcurrency, defaultOnBlockDone)
	zw.Reset(w)
	return zw
}

// Writer allows writing an LZ4 stream.
type Writer struct {
	state   _State
	src     io.Writer                 // destination writer
	level   lz4block.CompressionLevel // how hard to try
	num     int                       // concurrency level
	frame   *lz4stream.Frame          // frame being built
	data    []byte                    // pending data
	idx     int                       // size of pending data
	handler func(int)
	legacy  bool
}

func (*Writer) private() {}

func (w *Writer) Apply(options ...Option) (err error) {
	defer w.state.check(&err)
	switch w.state.state {
	case newState:
	case errorState:
		return w.state.err
	default:
		return lz4errors.ErrOptionClosedOrError
	}
	w.Reset(w.src)
	for _, o := range options {
		if err = o(w); err != nil {
			return
		}
	}
	return
}

func (w *Writer) isNotConcurrent() bool {
	return w.num == 1
}

// init sets up the Writer when in newState. It does not change the Writer state.
func (w *Writer) init() error {
	if !w.legacy && !w.frame.Descriptor.Flags.BlockSizeIndex().IsValid() {
		// Block8Mb is only valid in legacy frames, and LegacyOption may be
		// applied after BlockSizeOption.
		return fmt.Errorf("%w: %d is only valid for legacy frames", lz4errors.ErrOptionInvalidBlockSize, lz4block.Block8Mb)
	}
	w.frame.InitW(w.src, w.num, w.legacy)
	size := w.frame.BlockSizeIndex()
	w.data = size.Get()
	w.idx = 0
	return w.frame.Descriptor.Write(w.frame, w.src)
}

func (w *Writer) Write(buf []byte) (n int, err error) {
	if w.state.state == closedState {
		return 0, lz4errors.ErrWriterClosed
	}
	defer w.state.check(&err)
	switch w.state.state {
	case writeState:
	case errorState:
		return 0, w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return 0, w.state.fail()
	}

	zn := len(w.data)
	for len(buf) > 0 {
		if w.isNotConcurrent() && w.idx == 0 && len(buf) >= zn {
			// Avoid a copy as there is enough data for a block.
			if err = w.write(buf[:zn], false); err != nil {
				return
			}
			n += zn
			buf = buf[zn:]
			continue
		}
		// Accumulate the data to be compressed.
		m := copy(w.data[w.idx:], buf)
		n += m
		w.idx += m
		buf = buf[m:]

		if w.idx < len(w.data) {
			// Buffer not filled.
			return
		}

		// Buffer full.
		if err = w.write(w.data, true); err != nil {
			return
		}
		if !w.isNotConcurrent() {
			size := w.frame.BlockSizeIndex()
			w.data = size.Get()
		}
		w.idx = 0
	}
	return
}

func (w *Writer) write(data []byte, safe bool) error {
	if w.isNotConcurrent() {
		block := w.frame.Blocks.Block
		err := block.Compress(w.frame, data, w.level).Write(w.frame, w.src)
		w.handler(len(block.Data))
		return err
	}
	c := make(chan *lz4stream.FrameDataBlock)
	w.frame.Blocks.Blocks <- c
	go func(c chan *lz4stream.FrameDataBlock, data []byte, safe bool) {
		b := lz4stream.NewFrameDataBlock(w.frame)
		b.Compress(w.frame, data, w.level)
		// Report the block before handing it over, so that Close, which waits
		// for every block to be written, returns after all the reports.
		w.handler(len(b.Data))
		c <- b
		<-c
		b.Close(w.frame)
		if safe {
			// safe to put it back as the last usage of it was FrameDataBlock.Write() called before c is closed
			lz4block.Put(data)
		}
	}(c, data, safe)

	return nil
}

// Flush any buffered data to the underlying writer immediately.
func (w *Writer) Flush() (err error) {
	switch w.state.state {
	case writeState:
	case errorState:
		return w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return nil
	}

	if w.idx > 0 {
		if err = w.write(w.data[:w.idx], true); err != nil {
			return err
		}
		if !w.isNotConcurrent() {
			size := w.frame.Descriptor.Flags.BlockSizeIndex()
			w.data = size.Get()
		}
		w.idx = 0
	}
	return nil
}

// Close closes the Writer, flushing any unwritten data to the underlying writer
// without closing it.
func (w *Writer) Close() error {
	if w.state.state == closedState {
		return nil
	}
	if err := w.Flush(); err != nil {
		return err
	}
	err := w.frame.CloseW(w.src, w.num)
	// It is now safe to free the buffer.
	lz4block.Put(w.data)
	w.data = nil
	w.state.next(err)
	return err
}

// Cancel aborts the current writing session without trying to finish the
// frame: no more blocks are accepted, any buffered data is discarded, and
// the frame end mark is not written. The stream prefix produced so far is
// therefore left explicitly incomplete; readers will report an unexpected
// end of stream rather than a valid frame.
//
// Block compressions already in flight are drained: each of them returns
// the buffers it borrowed from the shared pools exactly once, whether it
// succeeded or failed, and results delivered after the cancellation are
// dropped instead of being written out. Only the buffers owned by this
// session are returned; the shared pools themselves are left untouched, so
// other writers are not affected.
//
// Cancel returns ErrWriterCanceled, unless the session had already failed,
// in which case the original error is kept as the cause. The Writer is left
// in its error state; call Reset to reuse it. Close is unaffected and keeps
// its usual behavior for non-canceled sessions.
func (w *Writer) Cancel() error {
	switch w.state.state {
	case closedState:
		return nil
	case errorState:
		// Keep the original error.
		return w.state.err
	}
	// Reject new blocks from now on.
	w.state.next(lz4errors.ErrWriterCanceled)
	// Drop late results: no block still in flight is written out.
	w.frame.StopW()
	// Discard the pending buffer: its data will never be flushed.
	lz4block.Put(w.data)
	w.data = nil
	w.idx = 0
	// Drain the pipeline and wait for every in-flight block to return its
	// buffers to the pool.
	if err := w.frame.CancelW(w.num); err != nil {
		// The session had already failed: keep the original cause.
		w.state.err = fmt.Errorf("%w: %w", lz4errors.ErrWriterCanceled, err)
	}
	return w.state.err
}

// Reset clears the state of the Writer w such that it is equivalent to its
// initial state from NewWriter, but instead writing to writer.
// Reset keeps the previous options unless overwritten by the supplied ones.
// No access to writer is performed.
//
// w.Close must be called before Reset or pending data may be dropped.
func (w *Writer) Reset(writer io.Writer) {
	lz4block.Put(w.data)
	w.data = nil
	w.frame.Reset(w.num)
	w.state.reset()
	w.src = writer
}

// ReadFrom efficiently reads from r and compressed into the Writer destination.
func (w *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	switch w.state.state {
	case closedState:
		return 0, lz4errors.ErrWriterClosed
	case errorState:
		return 0, w.state.err
	case newState:
		if err = w.init(); w.state.next(err) {
			return
		}
	default:
		return 0, w.state.fail()
	}
	defer w.state.check(&err)

	size := w.frame.BlockSizeIndex()
	var done bool
	var rn int
	data := size.Get()
	if w.isNotConcurrent() {
		// Keep the same buffer for the whole process.
		defer lz4block.Put(data)
	}
	for !done {
		rn, err = io.ReadFull(r, data)
		switch err {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF: // read may be partial
			err = nil
			done = true
		default:
			return
		}
		n += int64(rn)
		if rn > 0 {
			err = w.write(data[:rn], true)
			if err != nil {
				return
			}
		}
		if !done && !w.isNotConcurrent() {
			// The buffer will be returned automatically by go routines (safe=true)
			// so get a new one for the next round.
			data = size.Get()
		}
	}
	return
}
