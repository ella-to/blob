// Package pebble stores blobs inside a single Pebble (LSM) database instead of
// one file per blob.
//
// Layout:
//
//	'r' + ref                -> blob size (uint64, big endian)
//	'd' + ref + piece (u32)  -> piece of the blob content
//
// Blobs are split into pieces so large values stream on Get and play well with
// Pebble's block cache and value separation.
package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	pdb "github.com/cockroachdb/pebble/v2"

	"ella.to/blob"
	"ella.to/crypto"
	"ella.to/hash"
)

const DefaultPieceSize = 256 * 1024

const (
	prefixIndex = 'r'
	prefixData  = 'd'
)

type Storage struct {
	db        *pdb.DB
	opts      *pdb.Options
	write     *pdb.WriteOptions
	pieceSize int
	key       []byte
	buffers   sync.Pool
}

var (
	_ blob.Putter  = (*Storage)(nil)
	_ blob.Getter  = (*Storage)(nil)
	_ blob.Lister  = (*Storage)(nil)
	_ blob.Deleter = (*Storage)(nil)
)

// Put buffers the whole blob in memory, as the key is the hash of the content
// and is only known once the reader is drained.
func (s *Storage) Put(ctx context.Context, r io.Reader) (hash.Hash, int64, error) {
	buf := s.buffers.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.buffers.Put(buf)

	hr, getRef := hash.FromTeeReader(r)
	n, err := buf.ReadFrom(hr)
	if err != nil {
		return nil, 0, err
	} else if n == 0 {
		return nil, 0, io.EOF
	}

	ref := getRef()

	// content addressed: same ref means same content
	if _, closer, err := s.db.Get(indexKey(ref)); err == nil {
		_ = closer.Close()
		return ref, n, nil
	} else if !errors.Is(err, pdb.ErrNotFound) {
		return nil, 0, err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	data := buf.Bytes()
	for i := 0; len(data) > 0; i++ {
		piece := data[:min(len(data), s.pieceSize)]
		data = data[len(piece):]

		if len(s.key) > 0 {
			if piece, err = crypto.Encrypt(s.cryptoKey(), piece); err != nil {
				return nil, 0, err
			}
		}

		if err := batch.Set(dataKey(ref, uint32(i)), piece, nil); err != nil {
			return nil, 0, err
		}
	}

	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(n))
	if err := batch.Set(indexKey(ref), size[:], nil); err != nil {
		return nil, 0, err
	}

	if err := batch.Commit(s.write); err != nil {
		return nil, 0, err
	}

	return ref, n, nil
}

func (s *Storage) Get(ctx context.Context, ref hash.Hash) (io.ReadCloser, error) {
	lower, upper := dataRange(ref)

	it, err := s.db.NewIterWithContext(ctx, &pdb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}

	if !it.First() {
		err := it.Error()
		_ = it.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", blob.ErrNotFound, ref)
	}

	rd := &reader{it: it}
	if len(s.key) > 0 {
		key := s.cryptoKey()
		rd.key = &key
	}

	return rd, nil
}

func (s *Storage) List(ctx context.Context) iter.Seq2[hash.Hash, error] {
	return func(yield func(hash.Hash, error) bool) {
		it, err := s.db.NewIterWithContext(ctx, &pdb.IterOptions{
			LowerBound: []byte{prefixIndex},
			UpperBound: []byte{prefixIndex + 1},
		})
		if err != nil {
			yield(nil, err)
			return
		}
		defer it.Close()

		for valid := it.First(); valid; valid = it.Next() {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}

			ref, err := hash.ParseFromBytes(bytes.Clone(it.Key()[1:]))
			if !yield(ref, err) {
				return
			}
		}

		if err := it.Error(); err != nil {
			yield(nil, err)
		}
	}
}

func (s *Storage) Delete(ctx context.Context, ref hash.Hash) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	lower, upper := dataRange(ref)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	if err := batch.Delete(indexKey(ref), nil); err != nil {
		return err
	}

	return batch.Commit(s.write)
}

func (s *Storage) Close() error {
	return s.db.Close()
}

type reader struct {
	it      *pdb.Iterator
	key     *[32]byte
	pending []byte
	started bool
	err     error
}

func (r *reader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		r.err = r.load()
	}

	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *reader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		if len(r.pending) > 0 {
			n, err := w.Write(r.pending)
			total += int64(n)
			r.pending = r.pending[n:]
			if err != nil {
				return total, err
			}
		}

		if r.err != nil {
			if errors.Is(r.err, io.EOF) {
				return total, nil
			}
			return total, r.err
		}
		r.err = r.load()
	}
}

// load moves the next piece into pending. The iterator is already positioned
// on the first piece by Get. A piece is only valid until the iterator moves,
// so the iterator is advanced lazily.
func (r *reader) load() error {
	if r.started {
		r.it.Next()
	}
	r.started = true

	if !r.it.Valid() {
		if err := r.it.Error(); err != nil {
			return err
		}
		return io.EOF
	}

	value, err := r.it.ValueAndErr()
	if err != nil {
		return err
	}

	if r.key != nil {
		if value, err = crypto.Decrypt(*r.key, value); err != nil {
			return err
		}
	}

	r.pending = value
	return nil
}

func (r *reader) Close() error {
	r.err = io.ErrClosedPipe
	return r.it.Close()
}

func indexKey(ref hash.Hash) []byte {
	return append([]byte{prefixIndex}, ref...)
}

func dataKey(ref hash.Hash, piece uint32) []byte {
	key := make([]byte, 0, 1+len(ref)+4)
	key = append(key, prefixData)
	key = append(key, ref...)
	return binary.BigEndian.AppendUint32(key, piece)
}

func dataRange(ref hash.Hash) (lower, upper []byte) {
	lower = append([]byte{prefixData}, ref...)
	upper = append([]byte{prefixData}, ref...)
	return lower, append(upper, 0xff, 0xff, 0xff, 0xff, 0xff)
}

type Option func(*Storage)

// WithPieceSize sets the size of the pieces a blob is split into.
func WithPieceSize(size int) Option {
	return func(s *Storage) {
		s.pieceSize = size
	}
}

// WithSync makes every Put and Delete wait for the WAL to be synced to disk.
func WithSync(sync bool) Option {
	return func(s *Storage) {
		if sync {
			s.write = pdb.Sync
		} else {
			s.write = pdb.NoSync
		}
	}
}

// WithKey encrypts every piece at rest.
func WithKey(key string) Option {
	return func(s *Storage) {
		s.key = hash.FromBytes([]byte(key))
	}
}

// WithPebbleOptions overrides the options used to open the database.
func WithPebbleOptions(opts *pdb.Options) Option {
	return func(s *Storage) {
		s.opts = opts
	}
}

func Open(path string, options ...Option) (*Storage, error) {
	s := &Storage{
		write:     pdb.NoSync,
		pieceSize: DefaultPieceSize,
		buffers: sync.Pool{
			New: func() any { return new(bytes.Buffer) },
		},
	}

	for _, option := range options {
		option(s)
	}

	if s.pieceSize <= 0 {
		return nil, fmt.Errorf("piece size must be greater than zero")
	}

	if s.opts == nil {
		s.opts = defaultOptions()
	}

	db, err := pdb.Open(path, s.opts)
	if err != nil {
		return nil, err
	}

	s.db = db
	return s, nil
}

func defaultOptions() *pdb.Options {
	opts := &pdb.Options{
		FormatMajorVersion: pdb.FormatNewest,
		MemTableSize:       64 << 20,
		Logger:             quietLogger{},
	}

	// keep large pieces out of the LSM so compactions don't rewrite them
	opts.Experimental.ValueSeparationPolicy = func() pdb.ValueSeparationPolicy {
		return pdb.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           1024,
			MaxBlobReferenceDepth: 10,
			RewriteMinimumAge:     5 * time.Minute,
			TargetGarbageRatio:    0.2,
		}
	}

	return opts
}

// quietLogger drops pebble's informational messages, such as WAL recovery
// on every Open, and keeps errors.
type quietLogger struct{}

func (quietLogger) Infof(format string, args ...any) {}

func (quietLogger) Errorf(format string, args ...any) {
	pdb.DefaultLogger.Errorf(format, args...)
}

func (quietLogger) Fatalf(format string, args ...any) {
	pdb.DefaultLogger.Fatalf(format, args...)
}

func (s *Storage) cryptoKey() [32]byte {
	var key [32]byte
	copy(key[:], s.key)
	return key
}
