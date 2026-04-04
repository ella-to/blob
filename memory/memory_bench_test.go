package memory

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
)

// Benchmark Put operations with different data sizes
func BenchmarkMemoryStorage_Put(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1 * 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			storage := New()
			ctx := context.Background()
			data := make([]byte, s.size)
			rand.Read(data)

			b.ResetTimer()
			b.SetBytes(int64(s.size))

			for i := 0; i < b.N; i++ {
				_, _, err := storage.Put(ctx, bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark Get operations with different data sizes
func BenchmarkMemoryStorage_Get(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1 * 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			storage := New()
			ctx := context.Background()
			data := make([]byte, s.size)
			rand.Read(data)

			ref, _, err := storage.Put(ctx, bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(s.size))

			for i := 0; i < b.N; i++ {
				rc, err := storage.Get(ctx, ref)
				if err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})
	}
}

// Benchmark List operations with different numbers of items
func BenchmarkMemoryStorage_List(b *testing.B) {
	counts := []struct {
		name  string
		count int
	}{
		{"10items", 10},
		{"100items", 100},
		{"1000items", 1000},
	}

	for _, c := range counts {
		b.Run(c.name, func(b *testing.B) {
			storage := New()
			ctx := context.Background()

			// Pre-populate storage
			for i := 0; i < c.count; i++ {
				data := make([]byte, 100)
				rand.Read(data)
				_, _, err := storage.Put(ctx, bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				count := 0
				for range storage.List(ctx) {
					count++
				}
			}
		})
	}
}

// Benchmark concurrent Put operations
func BenchmarkMemoryStorage_ConcurrentPut(b *testing.B) {
	storage := New()
	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	b.ResetTimer()
	b.SetBytes(1024)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := storage.Put(ctx, bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark concurrent Get operations
func BenchmarkMemoryStorage_ConcurrentGet(b *testing.B) {
	storage := New()
	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	ref, _, err := storage.Put(ctx, bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(1024)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rc, err := storage.Get(ctx, ref)
			if err != nil {
				b.Fatal(err)
			}
			rc.Close()
		}
	})
}
