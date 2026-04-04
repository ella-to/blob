package blob

import (
	"context"
	"errors"
	"io"
	"iter"

	"ella.to/hash"
)

var ErrNotFound = errors.New("blob not found")

type Ref = hash.Hash

type Putter interface {
	Put(ctx context.Context, r io.Reader) (ref Ref, size int64, err error)
}

type Getter interface {
	Get(ctx context.Context, ref Ref) (rc io.ReadCloser, err error)
}

type Lister interface {
	List(ctx context.Context) iter.Seq2[Ref, error]
}

type Verifier interface {
	Verify(ctx context.Context, ref Ref) error
}

type Indexer interface {
	Index(ctx context.Context) error
}

type GetPutLister interface {
	Getter
	Putter
	Lister
}

func NewIterErr(err error) iter.Seq2[Ref, error] {
	return func(yield func(Ref, error) bool) {
		yield(nil, err)
	}
}
