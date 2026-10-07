package lz4

import (
	"fmt"
	"io"
	"sync"

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

	// Cancel session state. See Cancel for the resource lifecycle.
	cmu         sync.Mutex      // serializes block submission with cancellation
	cwg         sync.WaitGroup  // in-flight block compression routines
	cancelOnce  sync.Once       // guards the cancellation itself
	cancelCh    chan struct{}   // closed as soon as the session is canceled
	cancelDone  chan struct{}   // closed once cancel cleanup has completed
	cancelCause error           // final cancel error, valid once cancelDone is closed
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

// isCanceled reports whether the writing session has been canceled.
func (w *Writer) isCanceled() bool {
	select {
	case <-w.cancelCh:
		return true
	default:
		return false
	}
}

// Cancel explicitly aborts the writing session. It is safe to call it from
// another goroutine while Write, Flush or ReadFrom are blocked, and to call
// it more than once.
//
// Once Cancel has been called:
//   - no new block is accepted: Write, Flush and ReadFrom fail with
//     lz4errors.ErrWriterCanceled, and a Write blocked on a full block queue
//     is unblocked with that error;
//   - block compression routines that already hold buffers deliver their
//     result and return every buffer to the shared pool exactly once,
//     whether compression succeeded or the delivery was discarded;
//   - compressed results delivered after the cancellation are collected but
//     never written to the destination, so the already committed prefix is
//     left as a visibly incomplete frame: no end mark and no content
//     checksum are appended, and readers reject it instead of mistaking it
//     for a valid complete frame.
//
// Cancel waits until every in-flight block routine has released its buffers
// and the block collector has terminated; it does not interrupt a
// destination write that is already in progress. Only buffers are handed
// back to the shared pool, which is never closed, so other writers keep
// using it normally. Close and Reset behave as usual afterwards, except
// that Close reports the cancellation instead of completing the frame.
//
// The returned error is lz4errors.ErrWriterCanceled; if a destination write
// had already failed, that original cause is wrapped in it.
func (w *Writer) Cancel() error {
	if w.state.state == closedState {
		// The frame was already completed by a normal Close.
		return nil
	}
	w.cancelOnce.Do(func() {
		// Late compression results must not be written out. Flag the
		// collector first: once isCanceled reports the cancellation, the
		// collector is guaranteed to discard undelivered blocks.
		w.frame.Blocks.Cancel()
		// Reject new blocks: submissions check the channel while
		// holding cmu, so closing it here under the same lock guarantees
		// that no block routine is started after this point.
		w.cmu.Lock()
		close(w.cancelCh)
		w.cmu.Unlock()
		// Wait for every in-flight block routine to deliver its result and
		// return its buffers to the pool, then terminate the collector.
		w.cwg.Wait()
		err := w.frame.Blocks.Close(w.frame, w.num)
		if err != nil {
			// Keep the original cause of the failure.
			w.cancelCause = fmt.Errorf("%w: %w", lz4errors.ErrWriterCanceled, err)
		} else {
			w.cancelCause = lz4errors.ErrWriterCanceled
		}
		close(w.cancelDone)
	})
	<-w.cancelDone
	return w.cancelCause
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
	if w.isCanceled() {
		// The session was canceled: no new block is accepted.
		return 0, lz4errors.ErrWriterCanceled
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
	// Register the block routine before submitting it, and reject the block
	// if the session was canceled. The check and the registration happen
	// under cmu, and Cancel closes cancelCh under the same lock, so Cancel
	// waits for exactly the routines registered here.
	w.cmu.Lock()
	if w.isCanceled() {
		w.cmu.Unlock()
		return lz4errors.ErrWriterCanceled
	}
	w.cwg.Add(1)
	w.cmu.Unlock()
	select {
	case w.frame.Blocks.Blocks <- c:
	case <-w.cancelCh:
		// Canceled while the block queue was full: the block is not
		// accepted and its buffer stays with the caller.
		w.cwg.Done()
		return lz4errors.ErrWriterCanceled
	}
	go func(c chan *lz4stream.FrameDataBlock, data []byte, safe bool) {
		// Whatever happens to the compressed result, the buffers borrowed
		// for this block are returned to the shared pool exactly once.
		defer w.cwg.Done()
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
	if w.isCanceled() {
		return lz4errors.ErrWriterCanceled
	}
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
//
// If the session was canceled, Close does not attempt to complete the frame:
// it releases the pending buffer and reports the cancellation cause.
func (w *Writer) Close() error {
	if w.state.state == closedState {
		return nil
	}
	if w.isCanceled() {
		// The session was canceled: the committed prefix stays an
		// incomplete frame. Wait for the cancel cleanup to be done, then
		// release the pending buffer.
		err := w.Cancel()
		// Terminate the block collector if it was started by a Write
		// racing with Cancel; normally a no-op.
		_ = w.frame.Blocks.Close(w.frame, w.num)
		lz4block.Put(w.data)
		w.data = nil
		w.state.next(err)
		return err
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

// Reset clears the state of the Writer w such that it is equivalent to its
// initial state from NewWriter, but instead writing to writer.
// Reset keeps the previous options unless overwritten by the supplied ones.
// No access to writer is performed.
//
// w.Close must be called before Reset or pending data may be dropped.
// If the session was canceled, Cancel must have returned before Reset.
func (w *Writer) Reset(writer io.Writer) {
	lz4block.Put(w.data)
	w.data = nil
	w.frame.Reset(w.num)
	w.state.reset()
	w.src = writer
	// Start a fresh cancel session: the previous one, if any, is over and
	// all of its buffers have been returned to the shared pool.
	w.cancelOnce = sync.Once{}
	w.cancelCh = make(chan struct{})
	w.cancelDone = make(chan struct{})
	w.cancelCause = nil
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
	if w.isCanceled() {
		return 0, lz4errors.ErrWriterCanceled
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
				if !w.isNotConcurrent() {
					// The block was not accepted (the session was
					// canceled): the buffer was not handed over,
					// so it is still ours to return.
					lz4block.Put(data)
				}
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
