package merkle

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"ella.to/blob/local"
)

// Benchmark the merkle layer on top of real disk storage, which is how it
// is used in production. Memory backed benchmarks hide the cost of opening
// and reading blobs.
func BenchmarkMerkleLocal(b *testing.B) {
	pub, priv := getTestKeysBench(b)

	sizes := []struct {
		name string
		size int
	}{
		{"100KB", 100 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"64MB", 64 * 1024 * 1024},
	}

	for _, s := range sizes {
		data := make([]byte, s.size)
		_, _ = rand.Read(data)

		newStorage := func(b *testing.B) *Storage {
			m, err := New(
				WithStorage(local.NewStorage(local.WithPath(b.TempDir()))),
				WithChunckSize(1*1024*1024),
				WithKeys(pub, priv),
			)
			require.NoError(b, err)
			return m
		}

		b.Run("Put/"+s.name, func(b *testing.B) {
			m := newStorage(b)
			ctx := context.Background()

			b.SetBytes(int64(s.size))
			for b.Loop() {
				if _, _, err := m.Put(ctx, bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("Get/"+s.name, func(b *testing.B) {
			m := newStorage(b)
			ctx := context.Background()

			ref, _, err := m.Put(ctx, bytes.NewReader(data))
			require.NoError(b, err)

			b.SetBytes(int64(s.size))
			for b.Loop() {
				rc, err := m.Get(ctx, ref)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, rc); err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})

		b.Run("Verify/"+s.name, func(b *testing.B) {
			m := newStorage(b)
			ctx := context.Background()

			ref, _, err := m.Put(ctx, bytes.NewReader(data))
			require.NoError(b, err)

			b.SetBytes(int64(s.size))
			for b.Loop() {
				if err := m.Verify(ctx, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
