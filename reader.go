package lz4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

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

// Reader allows reading an LZ4 stream.
type Reader struct {
	state    _State
	src      io.Reader        // source reader
	count    *countingReader  // wraps src and counts compressed bytes consumed
	num      int              // concurrency level
	frame    *lz4stream.Frame // frame being read
	data     []byte           // block buffer allocated in non-concurrent mode
	reads    chan []byte      // pending data
	idx      int              // size of pending data
	handler  func(int)
	cum      uint32
	dict     []byte
	frameNum int // 1-based number of the frame being read, counting LZ4 frames only
	blockNum int // 1-based number of the block being read within the current frame
}

// countingReader records the number of compressed bytes consumed from the
// underlying reader. Skippable frames are read through it like any other
// bytes, so they count towards the compressed offset.
type countingReader struct {
	r      io.Reader
	offset int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.offset += int64(n)
	return n, err
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
	err := r.frame.ParseHeaders(r.src)
	if err != nil {
		return err
	}
	if !r.frame.Descriptor.Flags.BlockIndependence() {
		// We can't decompress dependent blocks concurrently.
		// Instead of throwing an error to the user, silently drop concurrency
		r.num = 1
	}
	data, err := r.frame.InitR(r.src, r.num)
	if err != nil {
		return err
	}
	r.reads = data
	r.idx = 0
	size := r.frame.BlockSizeIndex()
	r.data = size.Get()
	r.cum = 0
	r.frameNum++
	r.blockNum = 0
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
					err = r.frame.Blocks.ErrorR()
				}
			}
			switch err {
			case nil:
			case lz4errors.ErrEndOfStream:

				// Read Checksum.
				err = r.frame.CloseR(r.src)
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
				if er := r.frame.CloseR(r.src); er != nil {
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
	_, err := block.Read(r.frame, r.src, r.cum)
	if err != nil {
		return 0, err
	}
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
	r.blockNum++
	dst, err = block.Uncompress(r.frame, dst, r.dict, true)
	if err != nil {
		if errors.Is(err, lz4errors.ErrInvalidBlockChecksum) {
			return 0, &BlockChecksumError{
				Frame:            r.frameNum,
				Block:            r.blockNum,
				CompressedOffset: r.count.offset,
				Err:              err,
			}
		}
		return 0, err
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
// The frame, block and compressed-offset positions start over at zero: the
// new stream is measured independently of the previous one.
func (r *Reader) Reset(reader io.Reader) {
	lz4block.Put(r.data)
	r.data = nil
	r.frame.Reset(r.num)
	r.state.reset()
	r.count = &countingReader{r: reader}
	r.src = r.count
	r.reads = nil
	r.frameNum = 0
	r.blockNum = 0
}

// nextFrame prepares the Reader for the next frame of a concatenated stream
// on the same source. Unlike Reset, the compressed offset keeps accruing and
// the new frame takes the next frame number. Skippable frames skipped while
// parsing headers stay counted in the compressed offset.
func (r *Reader) nextFrame() {
	lz4block.Put(r.data)
	r.data = nil
	r.frame.Reset(r.num)
	r.state.reset()
	r.reads = nil
	r.blockNum = 0
}

// BlockChecksumError wraps an ErrInvalidBlockChecksum with the position of
// the corrupted block in the compressed stream.
type BlockChecksumError struct {
	// Frame is the 1-based number of the frame containing the bad block.
	// Skippable frames are not counted.
	Frame int
	// Block is the 1-based number of the bad block within its frame.
	Block int
	// CompressedOffset is the number of compressed bytes consumed from the
	// source when the checksum failure is detected: the position just past
	// the bad block including its checksum, before the frame end mark is
	// read. Skippable frames count.
	CompressedOffset int64
	// Err is the underlying error; errors.Is(err, ErrInvalidBlockChecksum)
	// keeps matching.
	Err error
}

func (e *BlockChecksumError) Error() string {
	detail := strings.TrimPrefix(e.Err.Error(), lz4errors.ErrInvalidBlockChecksum.Error())
	return fmt.Sprintf("%s at frame %d block %d compressed offset %d%s", lz4errors.ErrInvalidBlockChecksum, e.Frame, e.Block, e.CompressedOffset, detail)
}

func (e *BlockChecksumError) Unwrap() error { return e.Err }

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
				err = r.frame.Blocks.ErrorR()
			}
		}
		switch err {
		case nil:
		case lz4errors.ErrEndOfStream:
			// Read Checksum.
			err = r.frame.CloseR(r.src)
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
			err = r.frame.CloseR(r.src)
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
