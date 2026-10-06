package pebble_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"ella.to/blob"
	"ella.to/blob/merkle"
	"ella.to/blob/pebble"
	"ella.to/crypto"
)

func newStorage(t testing.TB, opts ...pebble.Option) *pebble.Storage {
	s, err := pebble.Open(t.TempDir(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestPutGet(t *testing.T) {
	sizes := []int{1, 100, pebble.DefaultPieceSize - 1, pebble.DefaultPieceSize, pebble.DefaultPieceSize*3 + 17}

	for _, encrypted := range []bool{false, true} {
		var opts []pebble.Option
		if encrypted {
			opts = append(opts, pebble.WithKey("secret"))
		}
		s := newStorage(t, opts...)
		ctx := context.Background()

		for _, size := range sizes {
			data := make([]byte, size)
			_, _ = rand.Read(data)

			ref, n, err := s.Put(ctx, iotest.HalfReader(bytes.NewReader(data)))
			require.NoError(t, err)
			require.Equal(t, int64(size), n)

			rc, err := s.Get(ctx, ref)
			require.NoError(t, err)
			got, err := io.ReadAll(iotest.OneByteReader(rc))
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.Equal(t, data, got)

			rc, err = s.Get(ctx, ref)
			require.NoError(t, err)
			var buf bytes.Buffer
			_, err = io.Copy(&buf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.Equal(t, data, buf.Bytes())
		}
	}
}

func TestPutEmpty(t *testing.T) {
	s := newStorage(t)
	_, _, err := s.Put(context.Background(), bytes.NewReader(nil))
	require.ErrorIs(t, err, io.EOF)
}

func TestPutIdempotent(t *testing.T) {
	s := newStorage(t)
	ctx := context.Background()

	ref1, _, err := s.Put(ctx, bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	ref2, _, err := s.Put(ctx, bytes.NewReader([]byte("hello")))
	require.NoError(t, err)
	require.Equal(t, ref1, ref2)

	count := 0
	for _, err := range s.List(ctx) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 1, count)
}

func TestListDelete(t *testing.T) {
	s := newStorage(t, pebble.WithPieceSize(16))
	ctx := context.Background()

	refs := make(map[string]bool)
	for i := range 20 {
		ref, _, err := s.Put(ctx, bytes.NewReader(bytes.Repeat([]byte{byte(i)}, 100)))
		require.NoError(t, err)
		refs[ref.String()] = true
	}

	listed := make(map[string]bool)
	for ref, err := range s.List(ctx) {
		require.NoError(t, err)
		listed[ref.String()] = true
	}
	require.Equal(t, refs, listed)

	for ref, err := range s.List(ctx) {
		require.NoError(t, err)
		require.NoError(t, s.Delete(ctx, ref))

		_, err := s.Get(ctx, ref)
		require.ErrorIs(t, err, blob.ErrNotFound)
	}

	for range s.List(ctx) {
		t.Fatal("storage should be empty")
	}

	// deleting a missing blob is not an error
	ref, _, err := s.Put(ctx, bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	require.NoError(t, s.Delete(ctx, ref))
	require.NoError(t, s.Delete(ctx, ref))
}

func TestReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := pebble.Open(dir)
	require.NoError(t, err)
	ref, _, err := s.Put(ctx, bytes.NewReader([]byte("persisted")))
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s, err = pebble.Open(dir)
	require.NoError(t, err)
	defer s.Close()

	rc, err := s.Get(ctx, ref)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "persisted", string(got))
}

func TestConcurrent(t *testing.T) {
	s := newStorage(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 50 {
				data := []byte{byte(i), byte(j)}
				ref, _, err := s.Put(ctx, bytes.NewReader(data))
				if !assertNoError(t, err) {
					return
				}
				rc, err := s.Get(ctx, ref)
				if !assertNoError(t, err) {
					return
				}
				got, err := io.ReadAll(rc)
				rc.Close()
				assertNoError(t, err)
				if !bytes.Equal(data, got) {
					t.Errorf("got %v, want %v", got, data)
				}
			}
		})
	}
	wg.Wait()
}

func assertNoError(t *testing.T, err error) bool {
	if err != nil {
		t.Error(err)
		return false
	}
	return true
}

func TestMerkleOnPebble(t *testing.T) {
	pub, priv, err := crypto.GenerateKey()
	require.NoError(t, err)

	s := newStorage(t, pebble.WithKey("secret"))
	m, err := merkle.New(
		merkle.WithStorage(s),
		merkle.WithChunckSize(64*1024),
		merkle.WithKeys(pub, priv),
		merkle.WithConcurrency(4),
	)
	require.NoError(t, err)

	ctx := context.Background()
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)

	root, _, err := m.Put(ctx, bytes.NewReader(data))
	require.NoError(t, err)
	require.NoError(t, m.Verify(ctx, root))

	rc, err := m.Get(ctx, root)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, data, got)

	require.NoError(t, m.Delete(ctx, root))
	_, err = m.GC(ctx)
	require.NoError(t, err)

	for range s.List(ctx) {
		t.Fatal("storage should be empty after GC")
	}
}
