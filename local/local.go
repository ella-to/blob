package local

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"ella.to/blob"
	"ella.to/crypto"
	"ella.to/hash"
)

type Storage struct {
	path string
	key  []byte
}

const defaultCryptoBlockSize = 1024

var (
	_ blob.Putter = (*Storage)(nil)
	_ blob.Getter = (*Storage)(nil)
	_ blob.Lister = (*Storage)(nil)
)

func (s *Storage) Put(ctx context.Context, r io.Reader) (ref hash.Hash, n int64, err error) {
	out, err := os.CreateTemp(s.path, "tmp-*")
	if err != nil {
		return nil, 0, err
	}

	defer func() {
		closeErr := out.Close()
		err = errors.Join(err, closeErr)

		if err == nil {
			renameErr := os.Rename(out.Name(), filepath.Join(s.path, ref.String()))
			if renameErr != nil {
				err = renameErr
				return
			}
		} else {
			removeErr := os.Remove(out.Name())
			err = errors.Join(err, removeErr)
		}
	}()

	// Use buffered writer for better performance
	bw := bufio.NewWriter(out)
	hr, getRef := hash.FromTeeReader(r)

	if len(s.key) > 0 {
		n, err = crypto.EncryptStream(s.cryptoKey(), defaultCryptoBlockSize, bw, hr)
	} else {
		n, err = io.Copy(bw, hr)
	}
	if err != nil {
		return nil, n, err
	} else if n == 0 {
		defer os.Remove(out.Name())
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
	file, err := os.Open(filepath.Join(s.path, r.String()))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %w: %s ", blob.ErrNotFound, err, r)
	}

	if err != nil {
		return nil, err
	}

	if len(s.key) == 0 {
		return file, nil
	}

	pr, pw := io.Pipe()
	go func() {
		_, decErr := crypto.DecryptStream(s.cryptoKey(), defaultCryptoBlockSize, pw, file)
		_ = file.Close()
		if decErr != nil {
			_ = pw.CloseWithError(decErr)
			return
		}
		_ = pw.Close()
	}()

	return pr, nil
}

func (s *Storage) List(ctx context.Context) iter.Seq2[hash.Hash, error] {
	type fileRef struct {
		blob hash.Hash
		err  error
	}

	files := make(chan *fileRef, 10)

	go func() {
		defer close(files)

		_ = filepath.WalkDir(s.path, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				files <- &fileRef{blob: nil, err: err}
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if d.IsDir() {
				return nil
			}

			r, err := hash.ParseFromString(d.Name())
			if err != nil {
				files <- &fileRef{blob: nil, err: err}
				return nil
			}

			files <- &fileRef{blob: r, err: nil}
			return nil
		})
	}()

	return func(yield func(hash.Hash, error) bool) {
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			case f, ok := <-files:
				if !ok {
					return
				}

				if !yield(f.blob, f.err) {
					return
				}
			}
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
