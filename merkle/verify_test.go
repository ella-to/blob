package merkle_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"ella.to/blob/local"
	"ella.to/blob/merkle"
)

func TestVerify_DetectsCorruptChunk(t *testing.T) {
	pub, priv := getTestKeys(t)

	for _, c := range []int{1, 4} {
		dir := t.TempDir()
		m, err := merkle.New(
			merkle.WithStorage(local.NewStorage(local.WithPath(dir))),
			merkle.WithChunckSize(1024),
			merkle.WithKeys(pub, priv),
			merkle.WithConcurrency(c),
		)
		require.NoError(t, err)

		ctx := context.Background()
		data := bytes.Repeat([]byte("abcdefgh"), 4096)
		ref, _, err := m.Put(ctx, bytes.NewReader(data))
		require.NoError(t, err)
		require.NoError(t, m.Verify(ctx, ref))

		// every chunk has the same content, so flipping one byte of it
		// corrupts the single chunk blob all leaves point to
		chunk := bytes.Repeat([]byte("abcdefgh"), 128)
		var path string
		require.NoError(t, filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				if b, _ := os.ReadFile(p); bytes.Equal(b, chunk) {
					path = p
				}
			}
			return err
		}))
		require.NotEmpty(t, path)

		chunk[0] ^= 0xff
		require.NoError(t, os.WriteFile(path, chunk, 0o644))
		require.Error(t, m.Verify(ctx, ref))
	}
}
