package merkle

import (
	"fmt"
	"io"

	"ella.to/crypto"
	"ella.to/hash"
)

// CalcRoot calculates the Merkle root from a stream without storing all leaves or nodes.
//
// The output root hash matches Storage.Put root calculation for the same chunkSize and
// childrenSize values.
func CalcRoot(r io.Reader, chunkSize int64, childrenSize int) (hash.Hash, int64, error) {
	return calcRoot(r, chunkSize, childrenSize, nil)
}

// CalcRootSigned calculates a Merkle root compatible with Storage.Put output by
// signing all internal nodes using privateKey before hashing the serialized node.
func CalcRootSigned(r io.Reader, chunkSize int64, childrenSize int, privateKey *crypto.PrivateKey) (hash.Hash, int64, error) {
	if privateKey == nil {
		return nil, 0, fmt.Errorf("private key is required")
	}

	return calcRoot(r, chunkSize, childrenSize, privateKey)
}

func calcRoot(r io.Reader, chunkSize int64, childrenSize int, privateKey *crypto.PrivateKey) (hash.Hash, int64, error) {
	if chunkSize <= 0 {
		return nil, 0, fmt.Errorf("chunk size must be greater than zero")
	}

	if childrenSize < 2 || childrenSize > MaxChildren {
		return nil, 0, fmt.Errorf("children size should be between 2 and %d", MaxChildren)
	}

	refs := make([]hash.Hash, 0)

	chunkBuf := make([]byte, chunkSize)
	var totalSize int64

	for {
		n, err := io.ReadFull(r, chunkBuf)
		if err == io.EOF {
			break
		}

		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, totalSize, err
		}

		if n > 0 {
			totalSize += int64(n)
			leafRef := hash.FromBytes(chunkBuf[:n])
			refs = append(refs, leafRef)
		}

		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	if totalSize == 0 {
		return nil, 0, ErrNothingToSave
	}

	levels := 1
	{
		refsCount := len(refs)
		capacity := childrenSize

		for refsCount > capacity {
			capacity *= childrenSize
			levels++
		}
	}

	for i := 0; i < levels; i++ {
		isRoot := i+1 == levels
		nextRefs := make([]hash.Hash, 0, (len(refs)+childrenSize-1)/childrenSize)

		for j := 0; j < len(refs); j += childrenSize {
			start := j
			end := start + childrenSize
			if end > len(refs) {
				end = len(refs)
			}

			node := &Node{
				IsRoot:   isRoot,
				Children: refs[start:end],
			}

			if privateKey == nil {
				nextRefs = append(nextRefs, node.Ref())
				continue
			}

			nodeReader := SignNodeReader(node, privateKey)
			nodeRef, err := hash.FromReader(nodeReader)
			if err != nil {
				return nil, totalSize, err
			}

			nextRefs = append(nextRefs, nodeRef)
		}

		refs = nextRefs
	}

	return refs[0], totalSize, nil
}
