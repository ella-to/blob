package merkle_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"ella.to/blob"
	"ella.to/blob/memory"
	"ella.to/blob/merkle"
	"ella.to/crypto"
)

func TestGet_Streaming(t *testing.T) {
	pub, priv := getTestKeys(t)

	m, err := merkle.New(
		merkle.WithStorage(memory.New()),
		merkle.WithChunckSize(100), // smaller than a node
		merkle.WithKeys(pub, priv),
	)
	require.NoError(t, err)

	ctx := t.Context()
	data := make([]byte, 10_000)
	rand.Read(data)

	ref, _, err := m.Put(ctx, bytes.NewReader(data))
	require.NoError(t, err)

	rc, err := m.Get(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, iotest.TestReader(rc, data))
	require.NoError(t, rc.Close())

	rc, err = m.Get(ctx, ref)
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rc)
	require.NoError(t, err)
	require.Equal(t, data, buf.Bytes())
	require.NoError(t, rc.Close())
}

func TestGet_NotFound(t *testing.T) {
	pub, priv := getTestKeys(t)

	m, err := merkle.New(merkle.WithStorage(memory.New()), merkle.WithKeys(pub, priv))
	require.NoError(t, err)

	_, err = m.Get(t.Context(), make(blob.Ref, 32))
	require.ErrorIs(t, err, blob.ErrNotFound)
}

func TestGet_Canceled(t *testing.T) {
	pub, priv := getTestKeys(t)

	m, err := merkle.New(
		merkle.WithStorage(memory.New()),
		merkle.WithChunckSize(1024),
		merkle.WithKeys(pub, priv),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	ref, _, err := m.Put(ctx, bytes.NewReader(make([]byte, 10*1024)))
	require.NoError(t, err)

	rc, err := m.Get(ctx, ref)
	require.NoError(t, err)
	defer rc.Close()

	cancel()
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, context.Canceled)
}

func TestVerify_WrongKey(t *testing.T) {
	pub, priv := getTestKeys(t)
	otherPub, _, err := crypto.GenerateKey()
	require.NoError(t, err)

	mem := memory.New()
	m, err := merkle.New(merkle.WithStorage(mem), merkle.WithKeys(pub, priv))
	require.NoError(t, err)

	ctx := t.Context()
	ref, _, err := m.Put(ctx, bytes.NewReader([]byte("hello")))
	require.NoError(t, err)

	other, err := merkle.New(merkle.WithStorage(mem), merkle.WithKeys(otherPub, nil))
	require.NoError(t, err)

	require.ErrorIs(t, other.Verify(ctx, ref), merkle.ErrInvalidNode)
	_, err = other.Get(ctx, ref)
	require.ErrorIs(t, err, merkle.ErrInvalidNode)
}
