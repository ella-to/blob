// Package bench compares the storage backends against each other.
//
//	go test -run '^$' -bench . -benchmem ./bench/
package bench_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"ella.to/blob"
	"ella.to/blob/local"
	"ella.to/blob/pebble"
)

type backend interface {
	blob.Putter
	blob.Getter
}

var backends = []struct {
	name string
	open func(b *testing.B) backend
}{
	{"local", func(b *testing.B) backend {
		return local.NewStorage(local.WithPath(b.TempDir()))
	}},
	{"pebble", func(b *testing.B) backend {
		s, err := pebble.Open(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { s.Close() })
		return s
	}},
	{"pebble-sync", func(b *testing.B) backend {
		s, err := pebble.Open(b.TempDir(), pebble.WithSync(true))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { s.Close() })
		return s
	}},
}

var sizes = []struct {
	name string
	size int
}{
	{"1KB", 1 << 10},
	{"64KB", 64 << 10},
	{"1MB", 1 << 20},
}

// unique returns a distinct payload per call, so every Put is a real write
// instead of hitting deduplication.
func unique(size int, seq uint64) []byte {
	data := make([]byte, size)
	_, _ = rand.Read(data)
	binary.BigEndian.PutUint64(data, seq)
	return data
}

func BenchmarkPut(b *testing.B) {
	for _, be := range backends {
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("%s/%s", be.name, sz.name), func(b *testing.B) {
				s := be.open(b)
				ctx := context.Background()
				data := unique(sz.size, 0)

				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				var seq uint64
				for b.Loop() {
					seq++
					binary.BigEndian.PutUint64(data, seq)
					if _, _, err := s.Put(ctx, bytes.NewReader(data)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkGet(b *testing.B) {
	for _, be := range backends {
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("%s/%s", be.name, sz.name), func(b *testing.B) {
				s := be.open(b)
				refs := populate(b, s, 256, sz.size)

				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					read(b, s, refs[i%len(refs)])
					i++
				}
			})
		}
	}
}

// Many small blobs written and read concurrently: the high traffic case.
func BenchmarkParallelPut4KB(b *testing.B) {
	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			s := be.open(b)
			ctx := context.Background()
			var seq atomic.Uint64

			b.SetBytes(4 << 10)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				data := unique(4<<10, 0)
				for pb.Next() {
					binary.BigEndian.PutUint64(data, seq.Add(1))
					if _, _, err := s.Put(ctx, bytes.NewReader(data)); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkParallelGet4KB(b *testing.B) {
	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			s := be.open(b)
			refs := populate(b, s, 10_000, 4<<10)
			var seq atomic.Uint64

			b.SetBytes(4 << 10)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					read(b, s, refs[seq.Add(7919)%uint64(len(refs))])
				}
			})
		})
	}
}

func populate(b *testing.B, s backend, count, size int) []blob.Ref {
	ctx := context.Background()
	refs := make([]blob.Ref, count)
	for i := range refs {
		ref, _, err := s.Put(ctx, bytes.NewReader(unique(size, uint64(i))))
		if err != nil {
			b.Fatal(err)
		}
		refs[i] = ref
	}
	return refs
}

func read(b *testing.B, s backend, ref blob.Ref) {
	rc, err := s.Get(context.Background(), ref)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, rc); err != nil {
		b.Fatal(err)
	}
	rc.Close()
}
