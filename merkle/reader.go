package merkle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"ella.to/blob"
)

var nodePrefix = []byte(`{"is_root":`)

// object is a blob classified by open. Exactly one of node and data is set;
// a valid tombstone is returned as data as well, so it reads like any blob.
type object struct {
	node      *Node
	tombstone *Tombstone
	data      io.ReadCloser
}

func (o *object) close() {
	if o.data != nil {
		_ = o.data.Close()
	}
}

// open fetches a blob once and classifies it. Merkle nodes and tombstones are
// always smaller than MaxNodeSize, so only a small prefix is read to tell
// them apart from data. For data blobs the returned reader yields the full
// content.
func (m *Storage) open(ctx context.Context, ref blob.Ref) (*object, error) {
	rc, err := m.storage.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, MaxNodeSize+1)
	n, err := io.ReadFull(rc, buf)
	switch {
	case err == nil:
		// bigger than any node, so it must be data
		return &object{data: &prefixReader{prefix: buf, rc: rc}}, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		_ = rc.Close()
	default:
		_ = rc.Close()
		return nil, err
	}

	buf = buf[:n]
	obj := &object{}

	if bytes.HasPrefix(buf, tombstonePrefix) {
		t := &Tombstone{}
		if json.Unmarshal(buf, t) == nil && t.Validate(m.publicKey) {
			obj.tombstone = t
		}
	} else if bytes.HasPrefix(buf, nodePrefix) {
		node, err := ParseNode(bytes.NewReader(buf))
		if err == nil {
			if !node.Validate(ctx, m.publicKey) {
				return nil, fmt.Errorf("%w: %s", ErrInvalidNode, ref)
			}
			obj.node = node
			return obj, nil
		}
	}

	obj.data = io.NopCloser(bytes.NewReader(buf))
	return obj, nil
}

// prefixReader replays the bytes already consumed by open before reading the
// rest of the blob.
type prefixReader struct {
	prefix []byte
	rc     io.ReadCloser
}

func (p *prefixReader) Read(b []byte) (int, error) {
	if len(p.prefix) > 0 {
		n := copy(b, p.prefix)
		p.prefix = p.prefix[n:]
		return n, nil
	}
	return p.rc.Read(b)
}

func (p *prefixReader) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.prefix)
	p.prefix = p.prefix[n:]
	if err != nil {
		return int64(n), err
	}
	m, err := io.Copy(w, p.rc)
	return int64(n) + m, err
}

func (p *prefixReader) Close() error {
	return p.rc.Close()
}

// treeReader streams the data chunks of a tree in order. Nodes are fetched
// lazily, so only one chunk is open at a time and no goroutine is needed.
type treeReader struct {
	ctx   context.Context
	m     *Storage
	stack [][]blob.Ref
	cur   io.ReadCloser
	err   error
}

func (t *treeReader) next() error {
	for {
		if err := t.ctx.Err(); err != nil {
			return err
		}

		for len(t.stack) > 0 && len(t.stack[len(t.stack)-1]) == 0 {
			t.stack = t.stack[:len(t.stack)-1]
		}

		if len(t.stack) == 0 {
			return io.EOF
		}

		top := &t.stack[len(t.stack)-1]
		ref := (*top)[0]
		*top = (*top)[1:]

		obj, err := t.m.open(t.ctx, ref)
		if err != nil {
			return err
		}

		if obj.node != nil {
			t.stack = append(t.stack, obj.node.Children)
			continue
		}

		t.cur = obj.data
		return nil
	}
}

func (t *treeReader) Read(p []byte) (int, error) {
	for {
		if t.err != nil {
			return 0, t.err
		}

		if t.cur == nil {
			if t.err = t.next(); t.err != nil {
				return 0, t.err
			}
		}

		n, err := t.cur.Read(p)
		if errors.Is(err, io.EOF) {
			err = t.cur.Close()
			t.cur = nil
			if err != nil {
				t.err = err
			}
			if n == 0 && err == nil {
				continue
			}
		} else if err != nil {
			t.err = err
		}

		return n, err
	}
}

func (t *treeReader) WriteTo(w io.Writer) (total int64, err error) {
	for {
		if t.err != nil {
			if errors.Is(t.err, io.EOF) {
				return total, nil
			}
			return total, t.err
		}

		if t.cur == nil {
			if t.err = t.next(); t.err != nil {
				continue
			}
		}

		n, err := io.Copy(w, t.cur)
		total += n
		closeErr := t.cur.Close()
		t.cur = nil
		if err = errors.Join(err, closeErr); err != nil {
			t.err = err
		}
	}
}

func (t *treeReader) Close() error {
	var err error
	if t.cur != nil {
		err = t.cur.Close()
		t.cur = nil
	}
	t.err = io.ErrClosedPipe
	return err
}
