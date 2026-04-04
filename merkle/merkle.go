package merkle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"ella.to/blob"
	"ella.to/crypto"
	"ella.to/hash"
)

var (
	ErrInvalidNode       = errors.New("invalid node")
	ErrNotNode           = errors.New("not a node")
	ErrNothingToSave     = errors.New("nothing to save")
	ErrIndexNotPerformed = errors.New("index not performed")
)

const (
	DefaultChunkSize    = 16 * 1024 * 1024
	DefaultChildrenSize = 2
)

type Storage struct {
	storage      blob.GetPutLister
	publicKey    *crypto.PublicKey
	privateKey   *crypto.PrivateKey
	childrenSize int
	chunckSize   int64
}

var (
	_ blob.Getter   = (*Storage)(nil)
	_ blob.Putter   = (*Storage)(nil)
	_ blob.Verifier = (*Storage)(nil)
)

func (m *Storage) Put(ctx context.Context, r io.Reader) (blob.Ref, int64, error) {
	var totalSize int64

	refs := make([]blob.Ref, 0)

	// save all data nodes
	for {
		ref, n, err := m.storage.Put(ctx, io.LimitReader(r, m.chunckSize))
		if errors.Is(err, io.EOF) || n == 0 {
			break
		} else if err != nil {
			return nil, totalSize, err
		}

		totalSize += n
		refs = append(refs, ref)
	}

	if len(refs) == 0 {
		return nil, 0, ErrNothingToSave
	}

	levels := 1

	{
		// Optimized level calculation using bit operations
		refsCount := len(refs)
		childSize := m.childrenSize
		capacity := childSize

		for refsCount > capacity {
			capacity *= childSize
			levels++
		}
	}

	var root blob.Ref

	// need to run this loop for all levels to calculate the root hash
	for i := 0; i < levels; i++ {
		isRoot := i+1 == levels

		nodes := make([]*Node, 0)

		for j := 0; j < len(refs); j += m.childrenSize {
			start := j
			end := start + m.childrenSize
			if end > len(refs) {
				end = len(refs)
			}

			node := &Node{}
			node.Children = refs[start:end]
			nodes = append(nodes, node)
		}

		refs = make([]blob.Ref, 0)
		for _, node := range nodes {
			node.IsRoot = isRoot
			ref, _, err := m.storage.Put(ctx, SignNodeReader(node, m.privateKey))
			if err != nil {
				return nil, totalSize, err
			}

			refs = append(refs, ref)
		}

		if isRoot {
			root = refs[0]
		}
	}

	return root, totalSize, nil
}

func (m *Storage) Get(ctx context.Context, r blob.Ref) (rc io.ReadCloser, err error) {
	pr, pw := io.Pipe()

	go func() {
		err := m.getHelper(ctx, r, pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return pr, nil
}

func (m *Storage) ListRootNodes(ctx context.Context) iter.Seq2[blob.Ref, error] {
	return func(yield func(blob.Ref, error) bool) {
		for ref, err := range m.storage.List(ctx) {
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				if !yield(nil, err) {
					return
				}
				continue
			}

			if ref == nil {
				continue
			}

			node, err := m.isValidMerkleNode(ctx, ref)
			if errors.Is(err, ErrNotNode) {
				continue
			} else if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			if node.IsRoot {
				if !yield(ref, nil) {
					return
				}
			}
		}
	}
}

func (m *Storage) ListRootChildrenNodes(ctx context.Context, ref blob.Ref, dataOnly bool) iter.Seq2[blob.Ref, error] {
	node, err := m.isValidMerkleNode(ctx, ref)
	if err != nil {
		return blob.NewIterErr(err)
	}

	queues := make([]blob.Ref, 0)
	queues = append(queues, node.Children...)

	return func(yield func(blob.Ref, error) bool) {
		for len(queues) > 0 {
			curr := queues[0]
			// move to the next item in the queue
			queues = queues[1:]

			if !dataOnly && !yield(curr, nil) {
				return
			}

			node, err := m.isValidMerkleNode(ctx, curr)
			if errors.Is(err, ErrNotNode) {
				if dataOnly {
					if !yield(curr, nil) {
						return
					}
				}
				// this happens when the node is not a merkle node
				// this should be data node, so we can safely ignore it
				continue
			} else if err != nil {
				if !dataOnly && !yield(nil, err) {
					return
				}
			}

			if node != nil {
				queues = append(queues, node.Children...)
			}
		}
	}
}

func (m *Storage) Verify(ctx context.Context, id blob.Ref) error {
	node, err := m.isValidMerkleNode(ctx, id)
	if err != nil {
		r, err := m.storage.Get(ctx, id)
		if err != nil {
			return err
		}
		defer r.Close()

		refValue, err := hash.FromReader(r)
		if err != nil {
			return err
		}

		if !bytes.Equal(id, refValue) {
			return fmt.Errorf("bad node %s, because it has invalid hash", id)
		}

		return nil
	}

	for _, child := range node.Children {
		if err := m.Verify(ctx, child); err != nil {
			return err
		}
	}

	return nil
}

func (m *Storage) isValidMerkleNode(ctx context.Context, ref blob.Ref) (node *Node, err error) {
	r, err := m.storage.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	node, err = ParseNode(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %w:%s", ErrNotNode, err, ref)
	}

	if !node.Validate(ctx, m.publicKey) {
		return nil, fmt.Errorf("%w: %s", ErrInvalidNode, ref)
	}

	return node, nil
}

func (m *Storage) getHelper(ctx context.Context, r blob.Ref, w io.Writer) error {
	var isData bool

	node, err := m.isValidMerkleNode(ctx, r)
	if errors.Is(err, ErrInvalidNode) {
		return err
	} else if err != nil {
		isData = true
	}

	if isData {
		chunk, err := m.storage.Get(ctx, r)
		if err != nil {
			return err
		}
		defer chunk.Close()

		_, err = io.Copy(w, chunk)
		if err != nil {
			return err
		}

		return nil
	} else {
		for _, child := range node.Children {
			if err := m.getHelper(ctx, child, w); err != nil {
				return err
			}
		}
	}

	return nil
}

type merkleOpt interface {
	configureMerkle(*Storage) error
}

type merkleOptFn func(*Storage) error

func (fn merkleOptFn) configureMerkle(m *Storage) error {
	return fn(m)
}

func WithStorage(storage blob.GetPutLister) merkleOptFn {
	return func(opts *Storage) error {
		opts.storage = storage
		return nil
	}
}

func WithKeys(publicKey *crypto.PublicKey, privateKey *crypto.PrivateKey) merkleOptFn {
	return func(opts *Storage) error {
		opts.publicKey = publicKey
		opts.privateKey = privateKey
		return nil
	}
}

func WithChildrenSize(size int) merkleOptFn {
	return func(opts *Storage) error {
		if size > MaxChildren {
			return fmt.Errorf("children size should be less or equal than %d", MaxChildren)
		}
		opts.childrenSize = size
		return nil
	}
}

func WithChunckSize(size int64) merkleOptFn {
	return func(opts *Storage) error {
		opts.chunckSize = size
		return nil
	}
}

func New(optsFn ...merkleOpt) (*Storage, error) {
	storage := &Storage{
		childrenSize: DefaultChildrenSize,
		chunckSize:   DefaultChunkSize,
	}

	for _, fn := range optsFn {
		if err := fn.configureMerkle(storage); err != nil {
			return nil, err
		}
	}

	if storage.storage == nil {
		return nil, errors.New("storage is required")
	}

	return storage, nil
}
