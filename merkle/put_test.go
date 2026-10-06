package merkle_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"ella.to/blob/memory"
	"ella.to/blob/merkle"
)

func TestPut_ConcurrencyMatchesSequential(t *testing.T) {
	pub, priv := getTestKeys(t)

	for _, size := range []int{1, 999, 1000, 1001, 33_333} {
		data := make([]byte, size)
		_, _ = rand.Read(data)

		want, _, err := merkle.CalcRootSigned(bytes.NewReader(data), 1000, 3, priv)
		require.NoError(t, err)

		for _, c := range []int{1, 2, 8} {
			t.Run(fmt.Sprintf("size=%d/c=%d", size, c), func(t *testing.T) {
				m, err := merkle.New(
					merkle.WithStorage(memory.New()),
					merkle.WithChunckSize(1000),
					merkle.WithChildrenSize(3),
					merkle.WithKeys(pub, priv),
					merkle.WithConcurrency(c),
				)
				require.NoError(t, err)

				ctx := context.Background()
				ref, n, err := m.Put(ctx, iotest.HalfReader(bytes.NewReader(data)))
				require.NoError(t, err)
				require.Equal(t, int64(size), n)
				require.Equal(t, want, ref)

				rc, err := m.Get(ctx, ref)
				require.NoError(t, err)
				got, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.Equal(t, data, got)
			})
		}
	}
}

func TestPut_ConcurrencyReadError(t *testing.T) {
	pub, priv := getTestKeys(t)

	m, err := merkle.New(
		merkle.WithStorage(memory.New()),
		merkle.WithChunckSize(10),
		merkle.WithKeys(pub, priv),
		merkle.WithConcurrency(4),
	)
	require.NoError(t, err)

	boom := errors.New("boom")
	r := io.MultiReader(bytes.NewReader(make([]byte, 100)), iotest.ErrReader(boom))

	_, _, err = m.Put(context.Background(), r)
	require.ErrorIs(t, err, boom)
}
