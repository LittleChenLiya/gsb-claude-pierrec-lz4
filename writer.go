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
	state    _State
	src      io.Writer                 // destination writer
	level    lz4block.CompressionLevel // how hard to try
	num      int                       // concurrency level
	frame    *lz4stream.Frame          // frame being built
	data     []byte                    // pending data
	idx      int                       // size of pending data
	handler  func(int)
	legacy   bool
	nwritten uint64 // uncompressed bytes accepted from Write and ReadFrom
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

// contentSize returns the declared original content size and whether the
// content size convention is in force. Legacy frames carry no descriptor and
// do not support it.
func (w *Writer) contentSize() (uint64, bool) {
	if w.legacy {
		return 0, false
	}
	return w.frame.Descriptor.ContentSize, w.frame.Descriptor.Flags.Size()
}

// limit truncates buf to the bytes still allowed by a declared content size.
// It returns the (possibly shortened) slice and the number of rejected bytes.
func (w *Writer) limit(buf []byte) ([]byte, int) {
	declared, ok := w.contentSize()
	if !ok {
		// Size unknown: no limit.
		return buf, 0
	}
	if w.nwritten >= declared {
		// Declared amount already accepted: everything is rejected.
		return buf[:0], len(buf)
	}
	remaining := declared - w.nwritten
	if uint64(len(buf)) <= remaining {
		return buf, 0
	}
	return buf[:remaining], int(uint64(len(buf)) - remaining)
}

// init sets up the Writer when in newState. It does not change the Writer state.
func (w *Writer) init() error {
	if !w.legacy && !w.frame.Descriptor.Flags.BlockSizeIndex().IsValid() {
		// Block8Mb is only valid in legacy frames, and LegacyOption may be
		// applied after BlockSizeOption.
		return fmt.Errorf("%w: %d is only valid for legacy frames", lz4errors.ErrOptionInvalidBlockSize, lz4block.Block8Mb)
	}
	w.frame.InitW(w.src, w.num, w.legacy)
	w.nwritten = 0
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
	// Enforce the declared original content size, if any: bytes past the
	// declared size are rejected immediately rather than only at Close.
	overrun := 0
	if allowed, rejected := w.limit(buf); rejected > 0 {
		buf = allowed
		overrun = rejected
	}
	for len(buf) > 0 {
		if w.isNotConcurrent() && w.idx == 0 && len(buf) >= zn {
			// Avoid a copy as there is enough data for a block.
			if err = w.write(buf[:zn], false); err != nil {
				return
			}
			n += zn
			w.nwritten += uint64(zn)
			buf = buf[zn:]
			continue
		}
		// Accumulate the data to be compressed.
		m := copy(w.data[w.idx:], buf)
		n += m
		w.nwritten += uint64(m)
		w.idx += m
		buf = buf[m:]

		if w.idx < len(w.data) {
			// Buffer not filled.
			break
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
	if overrun > 0 && err == nil {
		err = errContentSizeExceeded
	}
	return
}

// errContentSizeExceeded marks a write that would take in more bytes than the
// content size declared with SizeOption.
var errContentSizeExceeded = fmt.Errorf("%w: write exceeds declared content size", lz4errors.ErrInvalidContentSize)

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
		// Reject any bytes past the declared content size.
		overrun := 0
		if allowed, rejected := w.limit(data[:rn]); rejected > 0 {
			rn = len(allowed)
			overrun = rejected
			done = true
		}
		n += int64(rn)
		w.nwritten += uint64(rn)
		if rn > 0 {
			if err = w.write(data[:rn], true); err != nil {
				return
			}
		}
		if overrun > 0 && err == nil {
			err = errContentSizeExceeded
			return
		}
		if !done && !w.isNotConcurrent() {
			// The buffer will be returned automatically by go routines (safe=true)
			// so get a new one for the next round.
			data = size.Get()
		}
	}
	return
}
