package memory

import (
	"bytes"
	"context"
	"io"
	"iter"
	"maps"
	"slices"
	"sync"

	"ella.to/blob"
	"ella.to/hash"
)

type Storage struct {
	mu     sync.RWMutex
	mapper map[string][]byte
}

var (
	_ blob.Putter  = (*Storage)(nil)
	_ blob.Getter  = (*Storage)(nil)
	_ blob.Lister  = (*Storage)(nil)
	_ blob.Deleter = (*Storage)(nil)
)

func (s *Storage) Put(ctx context.Context, r io.Reader) (hash.Hash, int64, error) {
	r, getRef := hash.FromTeeReader(r)
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, err
	} else if len(b) == 0 {
		return nil, 0, io.EOF
	}

	ref := getRef()

	s.mu.Lock()
	s.mapper[string(ref)] = b
	s.mu.Unlock()

	return ref, int64(len(b)), nil
}

func (s *Storage) Get(ctx context.Context, r hash.Hash) (rc io.ReadCloser, err error) {
	s.mu.RLock()
	b, ok := s.mapper[string(r)]
	s.mu.RUnlock()

	if !ok {
		return nil, blob.ErrNotFound
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *Storage) Delete(ctx context.Context, r hash.Hash) error {
	s.mu.Lock()
	delete(s.mapper, string(r))
	s.mu.Unlock()

	return nil
}

func (s *Storage) List(ctx context.Context) iter.Seq2[hash.Hash, error] {
	s.mu.RLock()
	// sorted to make tests deterministic, as map order is random
	keys := slices.Sorted(maps.Keys(s.mapper))
	s.mu.RUnlock()

	return func(yield func(hash.Hash, error) bool) {
		for _, key := range keys {
			if !yield(hash.Hash(key), nil) {
				return
			}
		}

		yield(nil, io.EOF)
	}
}

func New() *Storage {
	return &Storage{
		mapper: make(map[string][]byte),
	}
}
