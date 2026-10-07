package merkle

import (
	"context"
	"io"
	"sync"

	"ella.to/blob"
)

// batch runs Puts on up to n goroutines and keeps the refs in submit order.
// With n <= 1 every Put runs inline.
type batch struct {
	ctx     context.Context
	storage blob.Putter
	sem     chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	refs    []blob.Ref
	err     error
}

func newBatch(ctx context.Context, storage blob.Putter, n int) *batch {
	b := &batch{ctx: ctx, storage: storage}
	if n > 1 {
		b.sem = make(chan struct{}, n)
	}
	return b
}

// put stores the reader returned by fn and calls done once the reader is no
// longer used.
func (b *batch) put(fn func() io.Reader, done func()) {
	b.mu.Lock()
	idx := len(b.refs)
	b.refs = append(b.refs, nil)
	b.mu.Unlock()

	run := func() {
		ref, _, err := b.storage.Put(b.ctx, fn())
		if done != nil {
			done()
		}

		b.mu.Lock()
		b.refs[idx] = ref
		if err != nil && b.err == nil {
			b.err = err
		}
		b.mu.Unlock()
	}

	if b.sem == nil {
		run()
		return
	}

	b.sem <- struct{}{}
	b.wg.Go(func() {
		defer func() { <-b.sem }()
		run()
	})
}

func (b *batch) failed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err != nil
}

func (b *batch) wait() ([]blob.Ref, error) {
	b.wg.Wait()
	return b.refs, b.err
}
