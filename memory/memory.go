package memory

import (
	"bytes"
	"context"
	"io"
	"iter"
	"sort"
	"sync"

	"ella.to/blob"
	"ella.to/hash"
)

type Storage struct {
	mu     sync.RWMutex
	mapper map[string][]byte
}

var (
	_ blob.Putter = (*Storage)(nil)
	_ blob.Getter = (*Storage)(nil)
	_ blob.Lister = (*Storage)(nil)
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

func (s *Storage) List(ctx context.Context) iter.Seq2[hash.Hash, error] {
	s.mu.RLock()
	keys := make([]hash.Hash, 0, len(s.mapper))
	for k := range s.mapper {
		keys = append(keys, hash.Hash(k))
	}
	s.mu.RUnlock()

	// sort keys to make test deterministic, as hash map is no deterministic
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})

	return func(yield func(hash.Hash, error) bool) {
		idx := 0

		for idx < len(keys) {
			ref := keys[idx]

			idx++
			if !yield(ref, nil) {
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
