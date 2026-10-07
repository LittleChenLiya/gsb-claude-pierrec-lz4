package lz4_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// errFailWriter is the deterministic error the failingWriter fixtures return.
var errFailWriter = errors.New("failingWriter: disk on fire")

// failingWriter accepts a prefix (the frame header) then fails every other
// write. afterN is the number of bytes allowed through before the first
// failure; zero fails immediately, before even the header is written.
type failingWriter struct {
	afterN int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.afterN > 0 {
		cut := len(p)
		if w.afterN < cut {
			cut = w.afterN
		}
		w.afterN -= cut
		if cut < len(p) {
			return cut, errFailWriter
		}
		return cut, nil
	}
	return 0, errFailWriter
}

// TestWriterSizeOptionExact writes exactly the declared 3 bytes and closes
// successfully; the frame round-trips through the existing Reader.
func TestWriterSizeOptionExact(t *testing.T) {
	var out bytes.Buffer
	zw := lz4.NewWriter(&out)
	if err := zw.Apply(lz4.SizeOption(3)); err != nil {
		t.Fatal(err)
	}
	if n, err := zw.Write([]byte("ab")); err != nil || n != 2 {
		t.Fatalf("first write: got (%d, %v), want (2, nil)", n, err)
	}
	if n, err := zw.Write([]byte("c")); err != nil || n != 1 {
		t.Fatalf("second write: got (%d, %v), want (1, nil)", n, err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close after exact write: %v", err)
	}

	zr := lz4.NewReader(&out)
	first := make([]byte, 1)
	if _, err := io.ReadFull(zr, first); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if size := zr.Size(); size != 3 {
		t.Fatalf("Reader.Size: got %d, want 3", size)
	}
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	got = append(first, got...)
	if string(got) != "abc" {
		t.Fatalf("round trip: got %q, want %q", got, "abc")
	}
}

// TestWriterSizeOptionShort closes after writing fewer than the declared 3
// bytes; Close reports the content size mismatch and the frame has no end mark.
func TestWriterSizeOptionShort(t *testing.T) {
	var out bytes.Buffer
	zw := lz4.NewWriter(&out)
	if err := zw.Apply(lz4.SizeOption(3)); err != nil {
		t.Fatal(err)
	}
	if n, err := zw.Write([]byte("ab")); err != nil || n != 2 {
		t.Fatalf("write: got (%d, %v), want (2, nil)", n, err)
	}
	err := zw.Close()
	if !errors.Is(err, lz4.ErrInvalidContentSize) {
		t.Fatalf("Close after short write: got %v, want ErrInvalidContentSize", err)
	}
	// The invalid frame must not carry the end-of-stream marker.
	if bytes.HasSuffix(out.Bytes(), []byte{0, 0, 0, 0}) {
		t.Fatalf("undersized frame was finalized with an end mark: % x", out.Bytes())
	}
}

// TestWriterSizeOptionLong rejects bytes past the declared 3 bytes at Write
// time: exactly 3 are accepted, the 4th byte is not, and Close keeps the
// content size error rather than finalizing the frame.
func TestWriterSizeOptionLong(t *testing.T) {
	var out bytes.Buffer
	zw := lz4.NewWriter(&out)
	if err := zw.Apply(lz4.SizeOption(3)); err != nil {
		t.Fatal(err)
	}
	n, err := zw.Write([]byte("abcd"))
	if !errors.Is(err, lz4.ErrInvalidContentSize) {
		t.Fatalf("overlong Write: got %v, want ErrInvalidContentSize", err)
	}
	if n != 3 {
		t.Fatalf("overlong Write accepted %d bytes, want 3", n)
	}
	// Once over the limit, further writes are rejected outright.
	if n, err := zw.Write([]byte("z")); !errors.Is(err, lz4.ErrInvalidContentSize) || n != 0 {
		t.Fatalf("write after overrun: got (%d, %v), want (0, ErrInvalidContentSize)", n, err)
	}
	if err := zw.Close(); !errors.Is(err, lz4.ErrInvalidContentSize) {
		t.Fatalf("Close after overrun: got %v, want ErrInvalidContentSize", err)
	}
}

// TestWriterSizeOptionLongSplit shows the rejection also triggers when the
// overrun arrives in a separate, smaller Write call.
func TestWriterSizeOptionLongSplit(t *testing.T) {
	var out bytes.Buffer
	zw := lz4.NewWriter(&out)
	if err := zw.Apply(lz4.SizeOption(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write([]byte("cd")); !errors.Is(err, lz4.ErrInvalidContentSize) {
		t.Fatalf("split overrun write: got %v, want ErrInvalidContentSize", err)
	}
}

// TestWriterSizeOptionUnderlyingErrorPriority checks that an error from the
// underlying output writer is preserved instead of being masked by the
// content size check, covering failure points at the descriptor write, a
// flushed partial block on a short Close, and a full block before an overrun.
func TestWriterSizeOptionUnderlyingErrorPriority(t *testing.T) {
	t.Run("short/flushBlock", func(t *testing.T) {
		// The descriptor goes through but the last partial block, flushed by
		// Close on a short frame, fails to write.
		fw := &failingWriter{afterN: 15} // 4 magic + 2 flags + 8 size + 1 checksum
		zw := lz4.NewWriter(fw)
		if err := zw.Apply(lz4.SizeOption(3), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
			t.Fatal(err)
		}
		if _, err := zw.Write([]byte("ab")); err != nil {
			t.Fatalf("write: %v", err)
		}
		err := zw.Close()
		if !errors.Is(err, errFailWriter) {
			t.Fatalf("Close: got %v, want underlying errFailWriter", err)
		}
		if errors.Is(err, lz4.ErrInvalidContentSize) {
			t.Fatalf("underlying error was masked by content size error: %v", err)
		}
	})

	t.Run("long/header", func(t *testing.T) {
		// The descriptor write itself fails: the overrun check must not
		// replace the underlying write error.
		fw := &failingWriter{} // fails on the first write
		zw := lz4.NewWriter(fw)
		if err := zw.Apply(lz4.SizeOption(3)); err != nil {
			t.Fatal(err)
		}
		_, err := zw.Write([]byte("abcd"))
		if !errors.Is(err, errFailWriter) {
			t.Fatalf("overlong write into failing writer: got %v, want errFailWriter", err)
		}
		if errors.Is(err, lz4.ErrInvalidContentSize) {
			t.Fatalf("header write error was masked by content size error: %v", err)
		}
	})

	t.Run("long/block", func(t *testing.T) {
		// Through ReadFrom, full blocks are flushed before the source's tail
		// overruns the declared size. The descriptor goes through but the
		// first data block fails on the wire, so the output error wins over
		// the content size mismatch.
		fw := &failingWriter{afterN: 15}
		zw := lz4.NewWriter(fw)
		if err := zw.Apply(lz4.SizeOption(3), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
			t.Fatal(err)
		}
		_, err := zw.ReadFrom(bytes.NewReader(bytes.Repeat([]byte("x"), int(lz4.Block64Kb)+4)))
		if !errors.Is(err, errFailWriter) {
			t.Fatalf("overrun after failing block output: got %v, want errFailWriter", err)
		}
		if errors.Is(err, lz4.ErrInvalidContentSize) {
			t.Fatalf("block write error was masked by content size error: %v", err)
		}
	})
}

// TestWriterSizeOptionReadFrom exercises the same convention through the
// ReadFrom interface used by io.Copy: an exact copy closes cleanly while an
// oversized source is rejected mid-stream.
func TestWriterSizeOptionReadFrom(t *testing.T) {
	t.Run("exact", func(t *testing.T) {
		var out bytes.Buffer
		zw := lz4.NewWriter(&out)
		if err := zw.Apply(lz4.SizeOption(3)); err != nil {
			t.Fatal(err)
		}
		if n, err := zw.ReadFrom(bytes.NewReader([]byte("abc"))); err != nil || n != 3 {
			t.Fatalf("ReadFrom: got (%d, %v), want (3, nil)", n, err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	t.Run("long", func(t *testing.T) {
		var out bytes.Buffer
		zw := lz4.NewWriter(&out)
		if err := zw.Apply(lz4.SizeOption(3), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
			t.Fatal(err)
		}
		// Feed more than a block so intermediate flushes happen.
		n, err := zw.ReadFrom(bytes.NewReader(bytes.Repeat([]byte("x"), int(lz4.Block64Kb)+4)))
		if !errors.Is(err, lz4.ErrInvalidContentSize) {
			t.Fatalf("ReadFrom overrun: got (%d, %v), want ErrInvalidContentSize", n, err)
		}
		if n != 3 {
			t.Fatalf("ReadFrom accepted %d bytes, want 3", n)
		}
	})
}

// TestWriterSizeOptionTempFileRoundTrip demonstrates the end-to-end result on
// a small temporary-file fixture: the compressed frame is written to disk
// with a declared content size and restored with the existing Reader.
func TestWriterSizeOptionTempFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "content.lz4")

	want := []byte("abc") // 3 bytes, matching SizeOption(3)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := lz4.NewWriter(f)
	if err := zw.Apply(lz4.SizeOption(uint64(len(want)))); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compressed %d bytes into %d-byte frame at %s", len(want), fi.Size(), path)

	g, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	got, err := io.ReadAll(lz4.NewReader(g))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip: got %q, want %q", got, want)
	}
}
