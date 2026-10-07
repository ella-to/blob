# Guide

Runnable snippets. Error handling is shortened to `check(err)`.

```go
func check(err error) {
	if err != nil {
		panic(err)
	}
}
```

## Backends

All backends implement `blob.Putter`, `blob.Getter`, `blob.Lister` and `blob.Deleter`.

### Local (one file per blob)

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"ella.to/blob/local"
)

func main() {
	ctx := context.Background()

	s := local.NewStorage(
		local.WithPath("./data"),   // directory must exist
		local.WithKey("my-secret"), // optional: encrypt at rest
	)

	ref, size, err := s.Put(ctx, bytes.NewReader([]byte("hello world")))
	check(err)
	fmt.Println(ref, size) // stored at ./data/b9/sha256-b94d27...

	rc, err := s.Get(ctx, ref)
	check(err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	check(err)
	fmt.Println(string(data))

	for ref, err := range s.List(ctx) {
		check(err)
		fmt.Println(ref.Short())
	}

	check(s.Delete(ctx, ref))
}
```

Blobs live in `<path>/<first 2 hex chars of the hash>/<ref>`, so a folder holds
~1/256 of the blobs.

### Memory

Same API, nothing touches disk. Handy for tests.

```go
s := memory.New()
```

## Merkle trees

```go
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"ella.to/blob/local"
	"ella.to/blob/merkle"
	"ella.to/crypto"
)

func main() {
	ctx := context.Background()

	pub, priv, err := crypto.GenerateKey()
	check(err)

	m, err := merkle.New(
		merkle.WithStorage(local.NewStorage(local.WithPath("./data"))),
		merkle.WithKeys(pub, priv),
		merkle.WithChunckSize(4*1024*1024), // default 16MB
		merkle.WithChildrenSize(2),         // 2..4
		merkle.WithConcurrency(4),          // parallel Put/Verify, buffers 4 chunks
	)
	check(err)

	f, err := os.Open("big.iso")
	check(err)
	defer f.Close()

	root, size, err := m.Put(ctx, f)
	check(err)
	fmt.Println(root, size)

	rc, err := m.Get(ctx, root)
	check(err)
	_, err = io.Copy(os.Stdout, rc)
	check(err)
	rc.Close()

	check(m.Verify(ctx, root))

	for root, err := range m.ListRootNodes(ctx) {
		check(err)
		fmt.Println(root.Short())
	}
}
```

Root without storing anything (same root `Put` returns for these settings):

```go
root, size, err := merkle.CalcRootSigned(f, 4*1024*1024, 2, priv)
```

## Delete and GC

`Delete` writes a signed tombstone for a root. The tree disappears from
`ListRootNodes` right away but stays readable; `GC` then removes every chunk
and node that no live tree shares. The backend must implement `blob.Deleter`
(local and memory do).

```go
package main

import (
	"bytes"
	"context"
	"fmt"

	"ella.to/blob/memory"
	"ella.to/blob/merkle"
	"ella.to/crypto"
)

func main() {
	ctx := context.Background()

	pub, priv, err := crypto.GenerateKey()
	check(err)

	m, err := merkle.New(
		merkle.WithStorage(memory.New()),
		merkle.WithKeys(pub, priv),
		merkle.WithChunckSize(1024),
	)
	check(err)

	shared := bytes.Repeat([]byte("x"), 4096)
	a, _, err := m.Put(ctx, bytes.NewReader(append(shared, 'a')))
	check(err)
	b, _, err := m.Put(ctx, bytes.NewReader(append(shared, 'b')))
	check(err)

	check(m.Delete(ctx, a)) // tombstone only

	stats, err := m.GC(ctx) // removes a's blobs, keeps chunks shared with b
	check(err)
	fmt.Printf("live=%d deleted=%d removed=%d\n", stats.Roots, stats.Tombstones, stats.Deleted)

	check(m.Verify(ctx, b))

	_, err = m.Get(ctx, a)
	fmt.Println(err) // blob not found
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
```

- Putting a deleted tree again before GC revives it.
- `GC` blocks `Put` and `Delete` on the same `merkle.Storage` while it runs.
  Don't run it while another process writes to the same backend.
- An interrupted `GC` is finished by the next run.

## Benchmarks

```bash
go test -run '^$' -bench . -benchmem ./merkle/ ./local/
```
