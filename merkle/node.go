package merkle

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"ella.to/crypto"
	"ella.to/hash"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

const (
	MaxChildren = 4
	// type can be either "hash" or "data"
	MinNodeSize = len(`{"is_root":true,"children":[]}`) + hash.StringSize
	MaxNodeSize = len(`{"is_root":false,"signed":"","children":[]}`) +
		2*(crypto.SignOverhead+hash.ByteSize) + // hex encoded signature
		MaxChildren*(hash.StringSize+2) + (MaxChildren - 1) // 2 for quotes and 1 for comma
)

func ParseNode(r io.Reader) (*Node, error) {
	node := &Node{}
	if err := json.NewDecoder(io.LimitReader(r, int64(MaxNodeSize))).Decode(node); err != nil {
		return nil, err
	}

	return node, nil
}

// Node is a node in a Merkle tree.
// if the node is a data node, all children will be point to the data node
type Node struct {
	IsRoot   bool          `json:"is_root"`
	Signed   crypto.Signed `json:"signed,omitempty"`
	Children []hash.Hash   `json:"children"`
}

// JsonEncode encodes the node to json
// we do this because there is no guarantee that the order of the fields
// will be the consistent which results in different hash
func (n *Node) JsonEncode() []byte {
	buffer := bufferPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer bufferPool.Put(buffer)

	buffer.WriteString(`{"is_root":`)
	if n.IsRoot {
		buffer.WriteString("true")
	} else {
		buffer.WriteString("false")
	}

	if n.Signed != nil {
		buffer.WriteString(`,"signed":"`)
		buffer.WriteString(n.Signed.String())
		buffer.WriteString(`"`)
	}

	buffer.WriteString(`,"children":[`)
	for i, child := range n.Children {
		if i != 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(`"`)
		buffer.WriteString(child.String())
		buffer.WriteString(`"`)
	}

	buffer.WriteString("]}")

	// Make a copy since we're returning the buffer to the pool
	result := make([]byte, buffer.Len())
	copy(result, buffer.Bytes())
	return result
}

func (n Node) Ref() hash.Hash {
	// SignedHash should not be part of the hash
	// because if we do this, it makes it imposible to update the SignedHash
	n.Signed = nil
	return hash.FromBytes(n.JsonEncode())
}

func (n *Node) Validate(ctx context.Context, publicKey *crypto.PublicKey) bool {
	return verify(publicKey, n.Signed, n.Ref())
}

// verify checks that signed is the signature of ref.
func verify(publicKey *crypto.PublicKey, signed crypto.Signed, ref hash.Hash) bool {
	buffer := make([]byte, crypto.SignOverhead+hash.ByteSize)
	copy(buffer[:crypto.SignOverhead], signed)
	copy(buffer[crypto.SignOverhead:], ref)
	return publicKey.Verify(buffer)
}

func SignNodeReader(n *Node, privateKey *crypto.PrivateKey) io.Reader {
	nodeWithSign := &Node{
		IsRoot:   n.IsRoot,
		Signed:   privateKey.Sign(n.Ref()),
		Children: n.Children,
	}

	return bytes.NewReader(nodeWithSign.JsonEncode())
}
