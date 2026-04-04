package merkle

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"ella.to/blob/memory"
	"ella.to/crypto"
)

func getTestKeysBench(b *testing.B) (*crypto.PublicKey, *crypto.PrivateKey) {
	pub, err := crypto.ParsePublicKey("6e8d840b091ff5a0cebef5da0db6b3e5bed3abbcb246ee6f7ffdf8095a7aeb39299463e71bd4943331a875492dc9b2fe077e27c03356ae32e786d347e6846307")
	require.NoError(b, err)
	priv, err := crypto.ParsePrivateKey("c52da125c710a042a20faea74c5002f8dc07493e80dc060e53e68ef2dd5b9e34574af3ada95b8faa914ba29c851dd642409cc8b95780816b7f62f1409df9c1e5299463e71bd4943331a875492dc9b2fe077e27c03356ae32e786d347e6846307")
	require.NoError(b, err)
	return pub, priv
}

// Benchmark Put operations with different data sizes and chunk sizes
func BenchmarkMerkleStorage_Put(b *testing.B) {
	pub, priv := getTestKeysBench(b)

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

	chunkSizes := []struct {
		name      string
		chunkSize int64
	}{
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
		{"1MB", 1 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	for _, c := range chunkSizes {
		for _, s := range sizes {
			b.Run(c.name+"/"+s.name, func(b *testing.B) {
				memStorage := memory.New()
				merkleStorage, err := New(
					WithStorage(memStorage),
					WithChildrenSize(2),
					WithChunckSize(c.chunkSize),
					WithKeys(pub, priv),
				)
				require.NoError(b, err)

				ctx := context.Background()
				data := make([]byte, s.size)
				_, _ = rand.Read(data)

				b.ResetTimer()
				b.SetBytes(int64(s.size))

				for b.Loop() {
					_, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Benchmark Get operations with different data sizes
func BenchmarkMerkleStorage_Get(b *testing.B) {
	pub, priv := getTestKeysBench(b)

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
			memStorage := memory.New()
			merkleStorage, err := New(
				WithStorage(memStorage),
				WithChildrenSize(2),
				WithChunckSize(1*1024*1024),
				WithKeys(pub, priv),
			)
			require.NoError(b, err)

			ctx := context.Background()
			data := make([]byte, s.size)
			_, _ = rand.Read(data)

			ref, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
			require.NoError(b, err)

			b.ResetTimer()
			b.SetBytes(int64(s.size))

			for b.Loop() {
				rc, err := merkleStorage.Get(ctx, ref)
				if err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})
	}
}

// Benchmark Verify operations with different data sizes
func BenchmarkMerkleStorage_Verify(b *testing.B) {
	pub, priv := getTestKeysBench(b)

	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1 * 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1 * 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			memStorage := memory.New()
			merkleStorage, err := New(
				WithStorage(memStorage),
				WithChildrenSize(2),
				WithChunckSize(64*1024),
				WithKeys(pub, priv),
			)
			require.NoError(b, err)

			ctx := context.Background()
			data := make([]byte, s.size)
			_, _ = rand.Read(data)

			ref, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
			require.NoError(b, err)

			b.ResetTimer()

			for b.Loop() {
				err := merkleStorage.Verify(ctx, ref)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark different children sizes
func BenchmarkMerkleStorage_ChildrenSize(b *testing.B) {
	pub, priv := getTestKeysBench(b)

	childrenSizes := []int{2, 3, 4}
	data := make([]byte, 1*1024*1024) // 1MB
	_, _ = rand.Read(data)

	for _, children := range childrenSizes {
		b.Run("children="+string(rune(children+'0')), func(b *testing.B) {
			memStorage := memory.New()
			merkleStorage, err := New(
				WithStorage(memStorage),
				WithChildrenSize(children),
				WithChunckSize(64*1024),
				WithKeys(pub, priv),
			)
			require.NoError(b, err)

			ctx := context.Background()

			b.ResetTimer()
			b.SetBytes(int64(len(data)))

			for i := 0; i < b.N; i++ {
				_, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark ListRootNodes
func BenchmarkMerkleStorage_ListRootNodes(b *testing.B) {
	pub, priv := getTestKeysBench(b)
	memStorage := memory.New()
	merkleStorage, err := New(
		WithStorage(memStorage),
		WithChildrenSize(2),
		WithChunckSize(64*1024),
		WithKeys(pub, priv),
	)
	require.NoError(b, err)

	ctx := context.Background()

	// Create some root nodes
	for i := 0; i < 10; i++ {
		data := make([]byte, 1024)
		_, _ = rand.Read(data)
		_, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
		require.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		for range merkleStorage.ListRootNodes(ctx) {
			count++
		}
	}
}

// Benchmark ListRootChildrenNodes
func BenchmarkMerkleStorage_ListRootChildrenNodes(b *testing.B) {
	pub, priv := getTestKeysBench(b)
	memStorage := memory.New()
	merkleStorage, err := New(
		WithStorage(memStorage),
		WithChildrenSize(2),
		WithChunckSize(10*1024), // Small chunks to create more nodes
		WithKeys(pub, priv),
	)
	require.NoError(b, err)

	ctx := context.Background()
	data := make([]byte, 100*1024) // 100KB
	_, _ = rand.Read(data)

	ref, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
	require.NoError(b, err)

	b.Run("dataOnly=false", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for range merkleStorage.ListRootChildrenNodes(ctx, ref, false) {
				count++
			}
		}
	})

	b.Run("dataOnly=true", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for range merkleStorage.ListRootChildrenNodes(ctx, ref, true) {
				count++
			}
		}
	})
}

// Benchmark concurrent operations
func BenchmarkMerkleStorage_ConcurrentPut(b *testing.B) {
	pub, priv := getTestKeysBench(b)
	memStorage := memory.New()
	merkleStorage, err := New(
		WithStorage(memStorage),
		WithChildrenSize(2),
		WithChunckSize(1*1024*1024),
		WithKeys(pub, priv),
	)
	require.NoError(b, err)

	ctx := context.Background()
	data := make([]byte, 10*1024) // 10KB
	_, _ = rand.Read(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := merkleStorage.Put(ctx, bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCalcRoot(b *testing.B) {
	configs := []struct {
		name         string
		size         int
		chunkSize    int64
		childrenSize int
	}{
		{name: "1MB-ch64KB-c2", size: 1 * 1024 * 1024, chunkSize: 64 * 1024, childrenSize: 2},
		{name: "10MB-ch256KB-c2", size: 10 * 1024 * 1024, chunkSize: 256 * 1024, childrenSize: 2},
		{name: "10MB-ch256KB-c4", size: 10 * 1024 * 1024, chunkSize: 256 * 1024, childrenSize: 4},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			data := make([]byte, cfg.size)
			_, _ = rand.Read(data)

			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := CalcRoot(bytes.NewReader(data), cfg.chunkSize, cfg.childrenSize)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCalcRootSigned(b *testing.B) {
	_, priv := getTestKeysBench(b)

	configs := []struct {
		name         string
		size         int
		chunkSize    int64
		childrenSize int
	}{
		{name: "1MB-ch64KB-c2", size: 1 * 1024 * 1024, chunkSize: 64 * 1024, childrenSize: 2},
		{name: "10MB-ch256KB-c2", size: 10 * 1024 * 1024, chunkSize: 256 * 1024, childrenSize: 2},
		{name: "10MB-ch256KB-c4", size: 10 * 1024 * 1024, chunkSize: 256 * 1024, childrenSize: 4},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			data := make([]byte, cfg.size)
			_, _ = rand.Read(data)

			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := CalcRootSigned(bytes.NewReader(data), cfg.chunkSize, cfg.childrenSize, priv)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
