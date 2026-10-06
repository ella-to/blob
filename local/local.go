package local

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"ella.to/blob"
	"ella.to/crypto"
	"ella.to/hash"
)

// Storage keeps every blob in its own file under a folder named after the
// first byte of its hash (2 hex chars), so a folder holds ~1/256 of the
// blobs: <path>/b9/sha256-b94d27...
//
// Blobs written by older versions directly under <path> are still read,
// listed and deleted; Migrate moves them into their folders.
type Storage struct {
	path string
	key  []byte
	dirs [256]atomic.Bool // shard folders known to exist

	legacyOnce sync.Once
	legacy     atomic.Bool // blobs in the old flat layout may exist
}

const (
	defaultCryptoBlockSize = 1024
	tmpPrefix              = "tmp-"
	ioBufferSize           = 64 * 1024
)

var writers = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, ioBufferSize) },
}

var (
	_ blob.Putter  = (*Storage)(nil)
	_ blob.Getter  = (*Storage)(nil)
	_ blob.Lister  = (*Storage)(nil)
	_ blob.Deleter = (*Storage)(nil)
)

func (s *Storage) Put(ctx context.Context, r io.Reader) (ref hash.Hash, n int64, err error) {
	out, err := os.CreateTemp(s.path, tmpPrefix+"*")
	if err != nil {
		return nil, 0, err
	}

	defer func() {
		if closeErr := out.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}

		if err == nil {
			renameErr := s.ensureDir(ref)
			if renameErr == nil {
				renameErr = os.Rename(out.Name(), s.blobPath(ref))
			}
			if renameErr != nil {
				err = renameErr
				return
			}
		} else {
			if removeErr := os.Remove(out.Name()); removeErr != nil {
				err = errors.Join(err, removeErr)
			}
		}
	}()

	// Use buffered writer for better performance
	bw := writers.Get().(*bufio.Writer)
	bw.Reset(out)
	defer func() {
		bw.Reset(nil)
		writers.Put(bw)
	}()
	hr, getRef := hash.FromTeeReader(r)

	if len(s.key) > 0 {
		n, err = crypto.EncryptStream(s.cryptoKey(), defaultCryptoBlockSize, bw, fullReader{hr})
	} else {
		n, err = copyBuffered(bw, hr)
	}
	if err != nil {
		return nil, n, err
	} else if n == 0 {
		return nil, n, io.EOF
	}

	// Flush buffer before renaming
	if errFlush := bw.Flush(); errFlush != nil {
		return nil, n, errFlush
	}

	ref = getRef()

	return ref, n, nil
}

func (s *Storage) Get(ctx context.Context, r hash.Hash) (rc io.ReadCloser, err error) {
	if len(r) != hash.ByteSize {
		return nil, fmt.Errorf("%w: invalid ref %x", blob.ErrNotFound, []byte(r))
	}

	file, err := os.Open(s.blobPath(r))
	if errors.Is(err, os.ErrNotExist) && s.hasLegacy() {
		file, err = os.Open(s.legacyPath(r))
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %w: %s ", blob.ErrNotFound, err, r)
	}

	if err != nil {
		return nil, err
	}

	if len(s.key) == 0 {
		return file, nil
	}

	return newDecryptReader(s.cryptoKey(), file), nil
}

func (s *Storage) Delete(ctx context.Context, r hash.Hash) error {
	if len(r) != hash.ByteSize {
		return nil
	}

	err := os.Remove(s.blobPath(r))
	if errors.Is(err, os.ErrNotExist) && s.hasLegacy() {
		err = os.Remove(s.legacyPath(r))
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

// Migrate moves blobs stored by older versions directly under the storage
// path into their shard folders.
func (s *Storage) Migrate(ctx context.Context) error {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		if entry.IsDir() {
			continue
		}

		ref, err := hash.ParseFromString(entry.Name())
		if err != nil {
			continue
		}

		if err := s.ensureDir(ref); err != nil {
			return err
		}

		if err := os.Rename(s.legacyPath(ref), s.blobPath(ref)); err != nil {
			return err
		}
	}

	s.legacyOnce.Do(func() {})
	s.legacy.Store(false)

	return nil
}

// hasLegacy reports whether the storage folder had blobs in the old flat
// layout when first checked, so new stores never pay for a second lookup.
func (s *Storage) hasLegacy() bool {
	s.legacyOnce.Do(func() {
		dir, err := os.Open(s.path)
		if err != nil {
			s.legacy.Store(true)
			return
		}
		defer dir.Close()

		for {
			entries, err := dir.ReadDir(256)
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasPrefix(entry.Name(), "sha256-") {
					s.legacy.Store(true)
					return
				}
			}
			if err != nil {
				return
			}
		}
	})

	return s.legacy.Load()
}

func (s *Storage) blobPath(ref hash.Hash) string {
	return filepath.Join(s.path, hex.EncodeToString(ref[:1]), ref.String())
}

func (s *Storage) legacyPath(ref hash.Hash) string {
	return filepath.Join(s.path, ref.String())
}

func (s *Storage) ensureDir(ref hash.Hash) error {
	if s.dirs[ref[0]].Load() {
		return nil
	}

	if err := os.MkdirAll(filepath.Join(s.path, hex.EncodeToString(ref[:1])), 0o755); err != nil {
		return err
	}

	s.dirs[ref[0]].Store(true)
	return nil
}

func (s *Storage) List(ctx context.Context) iter.Seq2[hash.Hash, error] {
	return func(yield func(hash.Hash, error) bool) {
		err := filepath.WalkDir(s.path, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			if d.IsDir() || strings.HasPrefix(d.Name(), tmpPrefix) {
				return nil
			}

			r, err := hash.ParseFromString(d.Name())
			if !yield(r, err) {
				return filepath.SkipAll
			}

			return nil
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

func WithPath(path string) func(*Storage) {
	return func(s *Storage) {
		s.path = path
	}
}

func WithKey(key string) func(*Storage) {
	return func(s *Storage) {
		s.key = hash.FromBytes([]byte(key))
	}
}

func NewStorage(options ...func(*Storage)) *Storage {
	s := &Storage{}
	for _, option := range options {
		option(s)
	}
	return s
}

func (s *Storage) cryptoKey() [32]byte {
	var key [32]byte
	copy(key[:], s.key)
	return key
}

// copyBuffered reads straight into the free space of bw. bufio.Writer's
// ReadFrom hands the copy to the file when its buffer is empty, and the file
// then allocates a buffer of its own on every call.
func copyBuffered(bw *bufio.Writer, r io.Reader) (int64, error) {
	var total int64
	for {
		if bw.Available() == 0 {
			if err := bw.Flush(); err != nil {
				return total, err
			}
		}

		buf := bw.AvailableBuffer()[:bw.Available()]
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = bw.Write(buf[:n]) // fits, so it only advances the buffer
			total += int64(n)
		}

		if errors.Is(err, io.EOF) {
			return total, nil
		} else if err != nil {
			return total, err
		}
	}
}

// fullReader fills the whole buffer on every Read unless the underlying
// reader is exhausted. Encrypted blobs are framed in fixed size blocks, so a
// short read would otherwise produce a block boundary that can't be decrypted.
type fullReader struct {
	r io.Reader
}

func (f fullReader) Read(p []byte) (int, error) {
	n, err := io.ReadFull(f.r, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = nil
	}
	return n, err
}

// decryptReader decrypts one block per refill, in the caller's goroutine.
type decryptReader struct {
	key     [32]byte
	src     io.Reader
	file    *os.File
	block   []byte
	pending []byte
	err     error
}

func newDecryptReader(key [32]byte, file *os.File) *decryptReader {
	return &decryptReader{
		key:   key,
		src:   bufio.NewReaderSize(file, ioBufferSize),
		file:  file,
		block: make([]byte, defaultCryptoBlockSize+crypto.EncryptionOverhead),
	}
}

func (d *decryptReader) Read(p []byte) (int, error) {
	for len(d.pending) == 0 {
		if d.err != nil {
			return 0, d.err
		}

		n, err := io.ReadFull(d.src, d.block)
		if n > 0 {
			d.pending, d.err = crypto.Decrypt(d.key, d.block[:n])
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		if d.err == nil {
			d.err = err
		}
	}

	n := copy(p, d.pending)
	d.pending = d.pending[n:]
	return n, nil
}

func (d *decryptReader) Close() error {
	return d.file.Close()
}
