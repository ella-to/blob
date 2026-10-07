package merkle_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"ella.to/blob"
	"ella.to/blob/memory"
	"ella.to/blob/merkle"
)

func newGCStorage(t *testing.T) (*merkle.Storage, *memory.Storage) {
	pub, priv := getTestKeys(t)
	mem := memory.New()
	m, err := merkle.New(
		merkle.WithStorage(mem),
		merkle.WithChunckSize(100),
		merkle.WithKeys(pub, priv),
	)
	require.NoError(t, err)
	return m, mem
}

func randomBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func countBlobs(t *testing.T, mem *memory.Storage) int {
	count := 0
	for ref, err := range mem.List(t.Context()) {
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotNil(t, ref)
		count++
	}
	return count
}

func listRoots(t *testing.T, m *merkle.Storage) []blob.Ref {
	roots := make([]blob.Ref, 0)
	for ref, err := range m.ListRootNodes(t.Context()) {
		require.NoError(t, err)
		roots = append(roots, ref)
	}
	return roots
}

func requireContent(t *testing.T, m *merkle.Storage, ref blob.Ref, want []byte) {
	rc, err := m.Get(t.Context(), ref)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, m.Verify(t.Context(), ref))
}

func TestGC_KeepsSharedBlobs(t *testing.T) {
	m, mem := newGCStorage(t)
	ctx := t.Context()

	shared := randomBytes(1000) // 10 chunks shared by both trees
	a := append(bytes.Clone(shared), randomBytes(500)...)
	b := append(bytes.Clone(shared), randomBytes(300)...)

	rootA, _, err := m.Put(ctx, bytes.NewReader(a))
	require.NoError(t, err)
	onlyA := countBlobs(t, mem)

	rootB, _, err := m.Put(ctx, bytes.NewReader(b))
	require.NoError(t, err)
	both := countBlobs(t, mem)

	require.NoError(t, m.Delete(ctx, rootA))
	require.Equal(t, []blob.Ref{rootB}, listRoots(t, m))

	// soft delete: still readable until GC
	requireContent(t, m, rootA, a)

	stats, err := m.GC(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Roots)
	require.Equal(t, 1, stats.Tombstones)

	requireContent(t, m, rootB, b)

	_, err = m.Get(ctx, rootA)
	require.ErrorIs(t, err, blob.ErrNotFound)

	// what is left is exactly B's tree
	rootBOnly, memBOnly := newGCStorage(t)
	_, _, err = rootBOnly.Put(ctx, bytes.NewReader(b))
	require.NoError(t, err)
	require.Equal(t, countBlobs(t, memBOnly), countBlobs(t, mem))
	require.Equal(t, both-countBlobs(t, mem)+1, stats.Deleted) // +1 for the tombstone
	require.Less(t, stats.Deleted, onlyA+1)                    // shared chunks survived

	stats, err = m.GC(ctx)
	require.NoError(t, err)
	require.Equal(t, merkle.GCStats{Roots: 1}, stats)
}

func TestGC_DeleteEverything(t *testing.T) {
	m, mem := newGCStorage(t)
	ctx := t.Context()

	roots := make([]blob.Ref, 0)
	for range 5 {
		root, _, err := m.Put(ctx, bytes.NewReader(randomBytes(1234)))
		require.NoError(t, err)
		roots = append(roots, root)
	}

	// same content twice is the same tree
	root, _, err := m.Put(ctx, bytes.NewReader(randomBytes(10)))
	require.NoError(t, err)
	roots = append(roots, root)

	for _, root := range roots {
		require.NoError(t, m.Delete(ctx, root))
		require.NoError(t, m.Delete(ctx, root)) // idempotent
	}
	require.Empty(t, listRoots(t, m))

	_, err = m.GC(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, countBlobs(t, mem))
}

func TestGC_PutRevivesDeletedRoot(t *testing.T) {
	m, _ := newGCStorage(t)
	ctx := t.Context()

	data := randomBytes(777)
	root, _, err := m.Put(ctx, bytes.NewReader(data))
	require.NoError(t, err)

	require.NoError(t, m.Delete(ctx, root))
	require.Empty(t, listRoots(t, m))

	again, _, err := m.Put(ctx, bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, root, again)
	require.Equal(t, []blob.Ref{root}, listRoots(t, m))

	stats, err := m.GC(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, stats.Deleted)
	requireContent(t, m, root, data)
}

func TestGC_ResumesPartialSweep(t *testing.T) {
	m, mem := newGCStorage(t)
	ctx := t.Context()

	root, _, err := m.Put(ctx, bytes.NewReader(randomBytes(2000)))
	require.NoError(t, err)
	require.NoError(t, m.Delete(ctx, root))

	// simulate a GC that crashed after removing part of the tree. GC removes
	// children before their parents, so drop the deepest half (the tail of
	// this breadth first listing)
	children := make([]blob.Ref, 0)
	for ref, err := range m.ListRootChildrenNodes(ctx, root, false) {
		require.NoError(t, err)
		children = append(children, ref)
	}
	for _, ref := range children[len(children)/2:] {
		require.NoError(t, mem.Delete(ctx, ref))
	}

	_, err = m.GC(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, countBlobs(t, mem))
}

func TestDelete_Errors(t *testing.T) {
	m, mem := newGCStorage(t)
	ctx := t.Context()

	root, _, err := m.Put(ctx, bytes.NewReader(randomBytes(1000)))
	require.NoError(t, err)

	for ref, err := range m.ListRootChildrenNodes(ctx, root, false) {
		require.NoError(t, err)
		require.ErrorIs(t, m.Delete(ctx, ref), merkle.ErrNotRoot)
	}

	require.ErrorIs(t, m.Delete(ctx, make(blob.Ref, 32)), blob.ErrNotFound)

	pub, _ := getTestKeys(t)
	readOnly, err := merkle.New(merkle.WithStorage(mem), merkle.WithKeys(pub, nil))
	require.NoError(t, err)
	require.ErrorIs(t, readOnly.Delete(ctx, root), merkle.ErrPrivateKeyRequired)
}

// noDelete hides the Delete method of the underlying storage.
type noDelete struct {
	blob.GetPutLister
}

func TestGC_RequiresDeleter(t *testing.T) {
	pub, priv := getTestKeys(t)
	m, err := merkle.New(merkle.WithStorage(noDelete{memory.New()}), merkle.WithKeys(pub, priv))
	require.NoError(t, err)

	_, err = m.GC(t.Context())
	require.ErrorIs(t, err, merkle.ErrDeleteNotSupported)
}

func TestGC_ConcurrentPut(t *testing.T) {
	m, _ := newGCStorage(t)
	ctx := t.Context()

	shared := randomBytes(1000)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 20 {
				data := append(bytes.Clone(shared), byte(i), byte(j))
				root, _, err := m.Put(ctx, bytes.NewReader(data))
				if err != nil {
					t.Error(err)
					return
				}
				if j%2 == 0 {
					if err := m.Delete(ctx, root); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}

	wg.Go(func() {
		for range 20 {
			if _, err := m.GC(ctx); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()

	_, err := m.GC(ctx)
	require.NoError(t, err)

	roots := listRoots(t, m)
	require.Len(t, roots, 8*10)
	for _, root := range roots {
		require.NoError(t, m.Verify(ctx, root))
	}
}
