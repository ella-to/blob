package merkle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"

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
	concurrency  int
	chunks       sync.Pool
}

var (
	_ blob.Getter   = (*Storage)(nil)
	_ blob.Putter   = (*Storage)(nil)
	_ blob.Verifier = (*Storage)(nil)
)

func (m *Storage) Put(ctx context.Context, r io.Reader) (blob.Ref, int64, error) {
	refs, totalSize, err := m.putChunks(ctx, r)
	if err != nil {
		return nil, totalSize, err
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

	// need to run this loop for all levels to calculate the root hash
	for i := 0; i < levels; i++ {
		isRoot := i+1 == levels

		b := newBatch(ctx, m.storage, m.concurrency)

		for j := 0; j < len(refs); j += m.childrenSize {
			node := &Node{
				IsRoot:   isRoot,
				Children: refs[j:min(j+m.childrenSize, len(refs))],
			}

			b.put(func() io.Reader { return SignNodeReader(node, m.privateKey) }, nil)
		}

		if refs, err = b.wait(); err != nil {
			return nil, totalSize, err
		}
	}

	return refs[0], totalSize, nil
}

// putChunks splits r into chunks and stores them. With concurrency > 1,
// chunks are buffered so that reading the next chunk overlaps with writing
// the previous ones.
func (m *Storage) putChunks(ctx context.Context, r io.Reader) ([]blob.Ref, int64, error) {
	var totalSize int64

	if m.concurrency <= 1 {
		refs := make([]blob.Ref, 0)
		for {
			ref, n, err := m.storage.Put(ctx, io.LimitReader(r, m.chunckSize))
			if errors.Is(err, io.EOF) || n == 0 {
				return refs, totalSize, nil
			} else if err != nil {
				return nil, totalSize, err
			}

			totalSize += n
			refs = append(refs, ref)
		}
	}

	b := newBatch(ctx, m.storage, m.concurrency)

	for !b.failed() {
		buf := m.chunks.Get().(*[]byte)

		n, err := io.ReadFull(r, *buf)
		if n > 0 {
			totalSize += int64(n)
			b.put(
				func() io.Reader { return bytes.NewReader((*buf)[:n]) },
				func() { m.chunks.Put(buf) },
			)
		} else {
			m.chunks.Put(buf)
		}

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			_, _ = b.wait()
			return nil, totalSize, err
		}
	}

	refs, err := b.wait()
	if err != nil {
		return nil, totalSize, err
	}

	return refs, totalSize, nil
}

func (m *Storage) Get(ctx context.Context, r blob.Ref) (rc io.ReadCloser, err error) {
	node, data, err := m.open(ctx, r)
	if err != nil {
		return nil, err
	}

	if data != nil {
		return data, nil
	}

	return &treeReader{ctx: ctx, m: m, stack: [][]blob.Ref{node.Children}}, nil
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

// Verify checks the signature of every node and the hash of every chunk
// under id. With concurrency > 1 chunks are verified in parallel.
func (m *Storage) Verify(ctx context.Context, id blob.Ref) error {
	sem := make(chan struct{}, max(1, m.concurrency))
	return m.verify(ctx, id, sem)
}

func (m *Storage) verify(ctx context.Context, id blob.Ref, sem chan struct{}) error {
	// the slot is only held while a blob is open, never while waiting for
	// children, so the tree can't deadlock the semaphore
	sem <- struct{}{}
	node, data, err := m.open(ctx, id)
	if err != nil {
		<-sem
		return err
	}

	if data != nil {
		defer func() { <-sem }()
		defer data.Close()

		refValue, err := hash.FromReader(data)
		if err != nil {
			return err
		}

		if !bytes.Equal(id, refValue) {
			return fmt.Errorf("bad node %s, because it has invalid hash", id)
		}

		return nil
	}
	<-sem

	if cap(sem) == 1 {
		for _, child := range node.Children {
			if err := m.verify(ctx, child, sem); err != nil {
				return err
			}
		}
		return nil
	}

	errs := make([]error, len(node.Children))
	var wg sync.WaitGroup
	for i, child := range node.Children {
		wg.Go(func() {
			errs[i] = m.verify(ctx, child, sem)
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (m *Storage) isValidMerkleNode(ctx context.Context, ref blob.Ref) (*Node, error) {
	node, data, err := m.open(ctx, ref)
	if err != nil {
		return nil, err
	}

	if data != nil {
		_ = data.Close()
		return nil, fmt.Errorf("%w: %s", ErrNotNode, ref)
	}

	return node, nil
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

// WithConcurrency sets how many chunks and nodes are written in parallel by
// Put. Values above 1 buffer up to that many chunks in memory.
func WithConcurrency(n int) merkleOptFn {
	return func(opts *Storage) error {
		opts.concurrency = n
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

	storage.chunks.New = func() any {
		buf := make([]byte, storage.chunckSize)
		return &buf
	}

	return storage, nil
}
