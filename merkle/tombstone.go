package merkle

import (
	"bytes"
	"io"

	"ella.to/crypto"
	"ella.to/hash"
)

var tombstonePrefix = []byte(`{"tombstone":`)

// Tombstone marks a root as deleted. It is stored as a regular blob, so it
// works with any backend, and it is signed, so only the owner of the private
// key can delete a tree. The tombstone of a root is deterministic, which lets
// Put revive a deleted root by removing its tombstone.
type Tombstone struct {
	Root   hash.Hash     `json:"tombstone"`
	Signed crypto.Signed `json:"signed,omitempty"`
}

func (t *Tombstone) JsonEncode() []byte {
	var buffer bytes.Buffer

	buffer.WriteString(`{"tombstone":"`)
	buffer.WriteString(t.Root.String())
	buffer.WriteString(`"`)

	if t.Signed != nil {
		buffer.WriteString(`,"signed":"`)
		buffer.WriteString(t.Signed.String())
		buffer.WriteString(`"`)
	}

	buffer.WriteString("}")

	return buffer.Bytes()
}

func (t Tombstone) Ref() hash.Hash {
	t.Signed = nil
	return hash.FromBytes(t.JsonEncode())
}

func (t *Tombstone) Validate(publicKey *crypto.PublicKey) bool {
	return verify(publicKey, t.Signed, t.Ref())
}

func SignTombstoneReader(root hash.Hash, privateKey *crypto.PrivateKey) io.Reader {
	t := &Tombstone{Root: root}
	t.Signed = privateKey.Sign(t.Ref())
	return bytes.NewReader(t.JsonEncode())
}

// tombstoneRef returns the ref the tombstone of root is stored under.
func tombstoneRef(root hash.Hash, privateKey *crypto.PrivateKey) (hash.Hash, error) {
	return hash.FromReader(SignTombstoneReader(root, privateKey))
}
