package lz4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/pierrec/lz4/v4/internal/lz4block"
	"github.com/pierrec/lz4/v4/internal/lz4errors"
	"github.com/pierrec/lz4/v4/internal/lz4stream"
)

var readerStates = []aState{
	noState:     newState,
	errorState:  newState,
	newState:    readState,
	readState:   closedState,
	closedState: newState,
}

// NewReader returns a new LZ4 frame decoder.
func NewReader(r io.Reader) *Reader {
	return newReader(r, false)
}

func newReader(r io.Reader, legacy bool) *Reader {
	zr := &Reader{frame: lz4stream.NewFrame()}
	zr.state.init(readerStates)
	_ = zr.Apply(DefaultConcurrency, defaultOnBlockDone)
	zr.Reset(r)
	return zr
}

// BlockChecksumError reports a block checksum failure with the position of
// the bad block in the compressed stream. The underlying checksum error is
// kept as the cause, so errors.Is(err, ErrInvalidBlockChecksum) matches.
type BlockChecksumError struct {
	Frame  int   // index of the frame in the stream, the first frame is 0
	Block  int   // index of the block in the frame, the first block is 0
	Offset int64 // compressed offset of the block in the stream
	Err    error // underlying block checksum error
}

func (e *BlockChecksumError) Error() string {
	return fmt.Sprintf("lz4: block checksum error at frame %d, block %d, compressed offset %d: %v",
		e.Frame, e.Block, e.Offset, e.Err)
}

func (e *BlockChecksumError) Unwrap() error { return e.Err }

// streamPos tracks the position of a Reader in its compressed stream: the
// frame and block being decoded and the compressed offset. It wraps the
// source reader so that every compressed byte, including skipped frames, is
// counted as it is consumed, without re-reading the stream.
type streamPos struct {
	src    io.Reader    // underlying source
	offset atomic.Int64 // compressed bytes consumed from src
	frame  int          // index of the frame being decoded
	block  int          // index of the block being decoded in the frame
}

func (p *streamPos) Read(buf []byte) (int, error) {
	n, err := p.src.Read(buf)
	p.offset.Add(int64(n))
	return n, err
}

// Reader allows reading an LZ4 stream.
type Reader struct {
	state   _State
	src     io.Reader        // source reader
	num     int              // concurrency level
	frame   *lz4stream.Frame // frame being read
	data    []byte           // block buffer allocated in non-concurrent mode
	reads   chan []byte      // pending data
	idx     int              // size of pending data
	handler func(int)
	cum     uint32
	dict    []byte
	pos     *streamPos // position in the compressed stream, for error context
}

func (*Reader) private() {}

func (r *Reader) Apply(options ...Option) (err error) {
	defer r.state.check(&err)
	switch r.state.state {
	case newState:
	case errorState:
		return r.state.err
	default:
		return lz4errors.ErrOptionClosedOrError
	}
	for _, o := range options {
		if err = o(r); err != nil {
			return
		}
	}
	return
}

// Size returns the size of the current frame's uncompressed data, if set in the stream.
func (r *Reader) Size() int {
	switch r.state.state {
	case readState, closedState:
		if r.frame.Descriptor.Flags.Size() {
			return int(r.frame.Descriptor.ContentSize)
		}
	}
	return 0
}

func (r *Reader) isNotConcurrent() bool {
	return r.num == 1
}

func (r *Reader) init() error {
	err := r.frame.ParseHeaders(r.pos)
	if err != nil {
		return err
	}
	r.pos.block = 0
	if !r.frame.Descriptor.Flags.BlockIndependence() {
		// We can't decompress dependent blocks concurrently.
		// Instead of throwing an error to the user, silently drop concurrency
		r.num = 1
	}
	data, err := r.frame.InitR(r.pos, r.num)
	if err != nil {
		return err
	}
	r.reads = data
	r.idx = 0
	size := r.frame.BlockSizeIndex()
	r.data = size.Get()
	r.cum = 0
	return nil
}

func (r *Reader) Read(buf []byte) (n int, err error) {
	defer r.state.check(&err)
	switch r.state.state {
	case readState:
	case closedState, errorState:
		return 0, r.state.err
	case newState:
		// First initialization.
		if err = r.init(); r.state.next(err) {
			return
		}
	default:
		return 0, r.state.fail()
	}
	for len(buf) > 0 {
		var bn int
		if r.idx == 0 {
		read:
			if r.isNotConcurrent() {
				bn, err = r.read(buf)
			} else {
				lz4block.Put(r.data)
				r.data = <-r.reads
				if len(r.data) == 0 {
					// No uncompressed data: something went wrong or we are done.
					err = r.blockError(r.pos.block, r.pos.offset.Load(), r.frame.Blocks.ErrorR())
				} else {
					r.pos.block++
				}
			}
			switch err {
			case nil:
			case lz4errors.ErrEndOfStream:

				// Read Checksum.
				err = r.frame.CloseR(r.pos)
				if err != nil {
					return
				}

				// Check for a new stream.
				r.nextFrame()
				if err = r.init(); r.state.next(err) {
					return
				}

				goto read
			case io.EOF:
				if er := r.frame.CloseR(r.pos); er != nil {
					err = er
				}
				lz4block.Put(r.data)
				r.data = nil
				// reset frame to release the buffer held by Block
				r.frame.Reset(r.num)
				return
			default:
				return
			}
		}
		if bn == 0 {
			// Fill buf with buffered data.
			bn = copy(buf, r.data[r.idx:])
			r.idx += bn
			if r.idx == len(r.data) {
				// All data read, get ready for the next Read.
				r.idx = 0
			}
		}
		buf = buf[bn:]
		n += bn
		r.handler(bn)
	}
	return
}

// read uncompresses the next block as follows:
//   - if buf has enough room, the block is uncompressed into it directly
//     and the length of used space is returned
//   - else, the uncompressed data is stored in r.data and 0 is returned
func (r *Reader) read(buf []byte) (int, error) {
	block := r.frame.Blocks.Block
	// Position of the block in the stream, for checksum error context.
	blockIdx, blockOffset := r.pos.block, r.pos.offset.Load()
	_, err := block.Read(r.frame, r.pos, r.cum)
	if err != nil {
		return 0, err
	}
	r.pos.block++
	var direct bool
	dst := r.data[:cap(r.data)]
	if len(buf) >= len(dst) {
		// Decompress directly into buf.
		// trim r.data as it is not needed now
		r.data = r.data[:0]
		direct = true
		// No more than a block, so that a block that decompresses to
		// more than the frame's block size is rejected as it is otherwise.
		dst = buf[:len(dst)]
	}
	dst, err = block.Uncompress(r.frame, dst, r.dict, true)
	if err != nil {
		return 0, r.blockError(blockIdx, blockOffset, err)
	}
	if !r.frame.Descriptor.Flags.BlockIndependence() {
		if len(r.dict)+len(dst) > 128*1024 {
			preserveSize := max(64*1024-len(dst), 0)
			r.dict = r.dict[len(r.dict)-preserveSize:]
		}
		r.dict = append(r.dict, dst...)
	}
	r.cum += uint32(len(dst))
	if direct {
		r.data = r.data[:0]
		return len(dst), nil
	}
	r.data = dst
	return 0, nil
}

// Reset clears the state of the Reader r such that it is equivalent to its
// initial state from NewReader, but instead reading from reader.
// No access to reader is performed.
func (r *Reader) Reset(reader io.Reader) {
	r.reset(reader)
	// A new stream: positions are computed from its start. A fresh tracker
	// is used as an abandoned concurrent read may still hold the old one.
	r.pos = &streamPos{src: reader}
}

func (r *Reader) reset(reader io.Reader) {
	lz4block.Put(r.data)
	r.data = nil
	r.frame.Reset(r.num)
	r.state.reset()
	r.src = reader
	r.reads = nil
}

// nextFrame resets the Reader to decode the next frame of the current
// stream, keeping the stream position so that error context offsets remain
// relative to the start of the stream.
func (r *Reader) nextFrame() {
	r.pos.frame++
	r.reset(r.src)
}

// blockError returns err unchanged unless it is a block checksum error, in
// which case it is wrapped with the position of the block in the stream.
func (r *Reader) blockError(block int, offset int64, err error) error {
	if !errors.Is(err, lz4errors.ErrInvalidBlockChecksum) {
		return err
	}
	return &BlockChecksumError{
		Frame:  r.pos.frame,
		Block:  block,
		Offset: offset,
		Err:    err,
	}
}

// WriteTo efficiently uncompresses the data from the Reader underlying source to w.
func (r *Reader) WriteTo(w io.Writer) (n int64, err error) {
	switch r.state.state {
	case readState:
	case closedState, errorState:
		return 0, r.state.err
	case newState:
		if err = r.init(); r.state.next(err) {
			if err == io.EOF {
				// An empty source is an empty stream; WriteTo reports
				// success rather than io.EOF.
				err = nil
			}
			return
		}
	default:
		return 0, r.state.fail()
	}
	defer r.state.nextd(&err)

	if r.idx > 0 {
		// A previous Read left part of the current block unconsumed.
		var bn int
		bn, err = w.Write(r.data[r.idx:])
		n += int64(bn)
		r.idx = 0
		if err != nil {
			return
		}
		r.handler(bn)
	}

	for {
		var bn int
		var dst []byte
	read:
		if r.isNotConcurrent() {
			// Uncompress into r.data, which init sizes for every frame.
			_, err = r.read(nil)
			dst = r.data
			bn = len(dst)
		} else {
			lz4block.Put(dst)
			dst = <-r.reads
			bn = len(dst)
			if bn == 0 {
				// No uncompressed data: something went wrong or we are done.
				err = r.blockError(r.pos.block, r.pos.offset.Load(), r.frame.Blocks.ErrorR())
			} else {
				r.pos.block++
			}
		}
		switch err {
		case nil:
		case lz4errors.ErrEndOfStream:
			// Read Checksum.
			err = r.frame.CloseR(r.pos)
			if err != nil {
				return
			}
			// Check for a new stream.
			r.nextFrame()
			if err = r.init(); r.state.next(err) {
				if err == io.EOF {
					err = nil
				}
				return
			}
			goto read
		case io.EOF:
			err = r.frame.CloseR(r.pos)
			return
		default:
			return
		}
		r.handler(bn)
		bn, err = w.Write(dst)
		n += int64(bn)
		if err != nil {
			return
		}
	}
}

// ValidFrameHeader returns a bool indicating if the given bytes slice matches a LZ4 header.
func ValidFrameHeader(in []byte) (bool, error) {
	f := lz4stream.NewFrame()
	err := f.ParseHeaders(bytes.NewReader(in))
	if err == nil {
		return true, nil
	}
	if err == lz4errors.ErrInvalidFrame {
		return false, nil
	}
	return false, err
}
