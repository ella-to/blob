package merkle_test

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"ella.to/blob/memory"
	"ella.to/blob/merkle"
)

func TestCalcRoot_MatchesStoragePut(t *testing.T) {
	pub, priv := getTestKeys(t)

	testCases := []struct {
		name         string
		dataSize     int
		chunkSize    int64
		childrenSize int
	}{
		{name: "small-single-chunk", dataSize: 11, chunkSize: 1024, childrenSize: 2},
		{name: "many-chunks-binary", dataSize: 1024 * 33, chunkSize: 256, childrenSize: 2},
		{name: "ternary-tree", dataSize: 1024 * 17, chunkSize: 300, childrenSize: 3},
		{name: "max-children", dataSize: 1024 * 25, chunkSize: 200, childrenSize: 4},
		{name: "last-partial-chunk", dataSize: 5000, chunkSize: 1024, childrenSize: 2},
	}

	rng := rand.New(rand.NewSource(42))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.dataSize)
			_, err := rng.Read(data)
			require.NoError(t, err)

			mem := memory.New()
			m, err := merkle.New(
				merkle.WithStorage(mem),
				merkle.WithChildrenSize(tc.childrenSize),
				merkle.WithChunckSize(tc.chunkSize),
				merkle.WithKeys(pub, priv),
			)
			require.NoError(t, err)

			putRoot, putSize, err := m.Put(context.Background(), bytes.NewReader(data))
			require.NoError(t, err)

			calcRoot, calcSize, err := merkle.CalcRootSigned(bytes.NewReader(data), tc.chunkSize, tc.childrenSize, priv)
			require.NoError(t, err)

			assert.Equal(t, putSize, calcSize)
			assert.Equal(t, putRoot.String(), calcRoot.String())
		})
	}
}

func TestCalcRoot_Errors(t *testing.T) {
	_, _, err := merkle.CalcRoot(bytes.NewReader(nil), 1024, 2)
	assert.ErrorIs(t, err, merkle.ErrNothingToSave)

	_, _, err = merkle.CalcRoot(bytes.NewReader([]byte("abc")), 0, 2)
	assert.Error(t, err)

	_, _, err = merkle.CalcRoot(bytes.NewReader([]byte("abc")), 1, 1)
	assert.Error(t, err)

	_, _, err = merkle.CalcRoot(bytes.NewReader([]byte("abc")), 1, 5)
	assert.Error(t, err)

	_, _, err = merkle.CalcRootSigned(bytes.NewReader([]byte("abc")), 1, 2, nil)
	assert.Error(t, err)
}
