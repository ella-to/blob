package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"
)

// Benchmark Put operations with different data sizes
func BenchmarkLocalStorage_Put(b *testing.B) {
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
			tmpDir := b.TempDir()
			storage := NewStorage(WithPath(tmpDir))
			ctx := context.Background()
			data := make([]byte, s.size)
			_, _ = rand.Read(data)

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
func BenchmarkLocalStorage_Get(b *testing.B) {
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
			tmpDir := b.TempDir()
			storage := NewStorage(WithPath(tmpDir))
			ctx := context.Background()
			data := make([]byte, s.size)
			_, _ = rand.Read(data)

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
func BenchmarkLocalStorage_List(b *testing.B) {
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
			tmpDir := b.TempDir()
			storage := NewStorage(WithPath(tmpDir))
			ctx := context.Background()

			// Pre-populate storage
			for i := 0; i < c.count; i++ {
				data := make([]byte, 100)
				_, _ = rand.Read(data)
				_, _, err := storage.Put(ctx, bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				count := 0
				for ref, err := range storage.List(ctx) {
					if ref == nil && err == nil {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
			}
		})
	}
}

// Benchmark concurrent Put operations
func BenchmarkLocalStorage_ConcurrentPut(b *testing.B) {
	tmpDir := b.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()
	data := make([]byte, 1024)
	_, _ = rand.Read(data)

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
func BenchmarkLocalStorage_ConcurrentGet(b *testing.B) {
	tmpDir := b.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()
	data := make([]byte, 1024)
	_, _ = rand.Read(data)

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

// Benchmark file system overhead
func BenchmarkLocalStorage_FileSystemOverhead(b *testing.B) {
	tmpDir := b.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	b.Run("CreateFile", func(b *testing.B) {
		data := []byte("test")
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ref, _, err := storage.Put(ctx, bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}
			// Clean up to avoid filling disk
			if i%100 == 0 {
				os.Remove(tmpDir + "/" + ref.String())
			}
		}
	})
}
