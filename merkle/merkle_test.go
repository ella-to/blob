package merkle_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/blob/memory"
	"ella.to/blob/merkle"
	"ella.to/crypto"
)

func getTestKeys(t *testing.T) (*crypto.PublicKey, *crypto.PrivateKey) {
	pub, err := crypto.ParsePublicKey("6e8d840b091ff5a0cebef5da0db6b3e5bed3abbcb246ee6f7ffdf8095a7aeb39299463e71bd4943331a875492dc9b2fe077e27c03356ae32e786d347e6846307")
	assert.NoError(t, err)
	priv, err := crypto.ParsePrivateKey("c52da125c710a042a20faea74c5002f8dc07493e80dc060e53e68ef2dd5b9e34574af3ada95b8faa914ba29c851dd642409cc8b95780816b7f62f1409df9c1e5299463e71bd4943331a875492dc9b2fe077e27c03356ae32e786d347e6846307")
	assert.NoError(t, err)

	return pub, priv
}

func TestBasicMerkle(t *testing.T) {
	memoryStorage := memory.New()

	pub, priv := getTestKeys(t)

	merkleStorage, err := merkle.New(
		merkle.WithStorage(memoryStorage),
		merkle.WithChildrenSize(2),
		merkle.WithChunckSize(10*1024*1024),
		merkle.WithKeys(pub, priv),
	)
	assert.NoError(t, err)

	content := []byte("hello world")

	root, n, err := merkleStorage.Put(context.Background(), bytes.NewReader(content))
	assert.NoError(t, err)
	assert.Equal(t, int64(11), n)
	assert.Equal(t, "sha256-931c1cc080775fbae890bbe06248c20866db7209e50e699c37c5e64c6770b28a", root.String())

	next, cancel := iter.Pull2(memoryStorage.List(context.Background()))
	defer cancel()

	ref1, err, ok := next()
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "sha256-931c1cc080775fbae890bbe06248c20866db7209e50e699c37c5e64c6770b28a", ref1.String())

	ref2, err, ok := next()
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "sha256-b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", ref2.String())

	ref3, err, ok := next()
	assert.True(t, ok)
	assert.ErrorIs(t, err, io.EOF)
	assert.Nil(t, ref3)

	rc, err := merkleStorage.Get(context.Background(), root)
	assert.NoError(t, err)
	defer rc.Close()

	b, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.Equal(t, content, b)

	err = merkleStorage.Verify(context.Background(), root)
	assert.NoError(t, err)
}

func TestMerkleIterateOverChildren(t *testing.T) {
	storage := memory.New()

	// localStoragePath := t.TempDir()
	// storage := local.NewStorage(localStoragePath)
	// fmt.Println(localStoragePath)

	pub, priv := getTestKeys(t)

	merkleStorage, err := merkle.New(
		merkle.WithStorage(storage),
		merkle.WithChildrenSize(2),
		merkle.WithChunckSize(1),
		merkle.WithKeys(pub, priv),
	)
	assert.NoError(t, err)

	data := make([]byte, 10)

	rootRef, size, err := merkleStorage.Put(context.Background(), bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), size)

	ctx := context.Background()

	results := make([]string, 0)

	for ref, err := range merkleStorage.ListRootNodes(ctx) {
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.NotNil(t, ref)
		results = append(results, ref.String())
	}

	preOrderList := []string{
		"sha256-d05013473dbc314c3521a81e53ece757380344055776b217bb67f14514421e85",
	}

	assert.Equal(t, preOrderList, results)

	rc, err := merkleStorage.Get(ctx, rootRef)
	assert.NoError(t, err)

	b, err := io.ReadAll(rc)
	assert.NoError(t, err)

	assert.Equal(t, data, b)

	{
		fmt.Println("list of children of root")
		i := 1
		for ref, err := range merkleStorage.ListRootChildrenNodes(ctx, rootRef, false) {
			if err == nil {
				fmt.Printf("%d\t- %s\n", i, ref.String())
				i++
			}
		}
	}

	{
		fmt.Println("list of data node only")
		i := 1
		for ref, err := range merkleStorage.ListRootChildrenNodes(ctx, rootRef, true) {
			if err == nil {
				fmt.Printf("%d\t- %s\n", i, ref.String())
				i++
			}
		}
	}
}
