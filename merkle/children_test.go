package merkle_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"ella.to/blob/memory"
	"ella.to/blob/merkle"
	"ella.to/hash"
)

func TestPutGet_AllChildrenSizes(t *testing.T) {
	pub, priv := getTestKeys(t)

	data := make([]byte, 10*1024+7)
	_, _ = rand.Read(data)

	for children := 2; children <= merkle.MaxChildren; children++ {
		t.Run(fmt.Sprintf("children=%d", children), func(t *testing.T) {
			m, err := merkle.New(
				merkle.WithStorage(memory.New()),
				merkle.WithChildrenSize(children),
				merkle.WithChunckSize(512),
				merkle.WithKeys(pub, priv),
			)
			require.NoError(t, err)

			ctx := context.Background()
			ref, _, err := m.Put(ctx, bytes.NewReader(data))
			require.NoError(t, err)

			rc, err := m.Get(ctx, ref)
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.Equal(t, data, got)

			require.NoError(t, m.Verify(ctx, ref))

			roots := 0
			for r, err := range m.ListRootNodes(ctx) {
				require.NoError(t, err)
				require.Equal(t, ref, r)
				roots++
			}
			require.Equal(t, 1, roots)
		})
	}
}

func TestMaxNodeSize(t *testing.T) {
	_, priv := getTestKeys(t)

	node := &merkle.Node{}
	for i := range merkle.MaxChildren {
		node.Children = append(node.Children, hash.FromBytes([]byte{byte(i)}))
	}

	b, err := io.ReadAll(merkle.SignNodeReader(node, priv))
	require.NoError(t, err)
	require.Equal(t, merkle.MaxNodeSize, len(b))
}
