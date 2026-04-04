```
██████╗░██╗░░░░░░█████╗░██████╗░
██╔══██╗██║░░░░░██╔══██╗██╔══██╗
██████╦╝██║░░░░░██║░░██║██████╦╝
██╔══██╗██║░░░░░██║░░██║██╔══██╗
██████╦╝███████╗╚█████╔╝██████╦╝
╚═════╝░╚══════╝░╚════╝░╚═════╝░
```
<div align="center">

[![Go Reference](https://pkg.go.dev/badge/ella.to/blob.svg)](https://pkg.go.dev/ella.to/blob)
[![Go Report Card](https://goreportcard.com/badge/ella.to/blob)](https://goreportcard.com/report/ella.to/blob)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**blob** is a content-addressable storage library with Merkle tree support, optional encryption, and pluggable backends.

</div>

## Installation

```bash
go get ella.to/blob@v0.0.1
```

## Overview

Blob provides a simple interface for storing and retrieving data by its SHA-256 hash. Data goes in, you get a reference back, and you can always retrieve the exact same data using that reference. The library ships with two storage backends (local filesystem and in-memory) and a Merkle tree layer for handling large files with integrity verification.

## Core Interfaces

The package defines a small set of interfaces that all backends implement:

```go
// Store data and get back its content hash
type Putter interface {
    Put(ctx context.Context, r io.Reader) (ref Ref, size int64, err error)
}

// Retrieve data by its content hash
type Getter interface {
    Get(ctx context.Context, ref Ref) (rc io.ReadCloser, err error)
}

// Iterate over all stored references
type Lister interface {
    List(ctx context.Context) iter.Seq2[Ref, error]
}
```

`Ref` is just a type alias for `hash.Hash` — a SHA-256 digest of the content.

## Local Storage

The `local` sub-package stores blobs as files on disk, named by their hash. It optionally encrypts content at rest.

```go
import "ella.to/blob/local"

// Plain storage
storage := local.NewStorage(
    local.WithPath("/var/data/blobs"),
)

// Encrypted storage — content is encrypted before writing to disk
storage := local.NewStorage(
    local.WithPath("/var/data/blobs"),
    local.WithKey("my-secret-key"),
)
```

### Storing and Retrieving Data

```go
ctx := context.Background()

// Store
ref, size, err := storage.Put(ctx, bytes.NewReader([]byte("hello world")))
// ref is the SHA-256 hash of the *original* data (computed before encryption)

// Retrieve
rc, err := storage.Get(ctx, ref)
defer rc.Close()
data, _ := io.ReadAll(rc)
```

Puts are idempotent — storing the same content twice produces the same ref and a single file on disk.

### Listing Blobs

```go
for ref, err := range storage.List(ctx) {
    if ref == nil && err == nil {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(ref.String())
}
```

## In-Memory Storage

Useful for tests or caching. Same interface, no disk involved.

```go
import "ella.to/blob/memory"

storage := memory.New()
ref, _, _ := storage.Put(ctx, bytes.NewReader(data))
rc, _ := storage.Get(ctx, ref)
```

## Merkle Tree Storage

The `merkle` sub-package adds a Merkle tree layer on top of any `GetPutLister` backend. Large data is split into chunks, organized into a tree of signed nodes, and stored as individual blobs. This gives you:

- Streaming reads for large files (no need to load everything into memory)
- Integrity verification at every level of the tree
- Tamper detection through cryptographic signatures on tree nodes

```go
import (
    "ella.to/blob/memory"
    "ella.to/blob/merkle"
    "ella.to/crypto"
)

pub, priv, _ := crypto.GenerateKey()
mem := memory.New()

m, err := merkle.New(
    merkle.WithStorage(mem),
    merkle.WithKeys(pub, priv),
    merkle.WithChunckSize(16 * 1024 * 1024), // 16 MB chunks (default)
    merkle.WithChildrenSize(2),                // binary tree (default)
)
```

### Storing and Reading

```go
// Store a large file — it gets chunked and organized into a tree
ref, totalSize, err := m.Put(ctx, file)

// Read it back — chunks are reassembled transparently
rc, err := m.Get(ctx, ref)
defer rc.Close()
data, _ := io.ReadAll(rc)
```

### Verification

Verify that all chunks and tree nodes are intact:

```go
err := m.Verify(ctx, ref)
```

### Listing Root Nodes

```go
for ref, err := range m.ListRootNodes(ctx) {
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("root:", ref.Short())
}
```

### Computing Merkle Root Without Storing

If you just need the root hash (e.g. for comparison) without actually storing the data:

```go
root, size, err := merkle.CalcRoot(reader, chunkSize, childrenSize)

// Or with signatures to match what Storage.Put would produce:
root, size, err := merkle.CalcRootSigned(reader, chunkSize, childrenSize, privateKey)
```

## How the Merkle Tree Works

1. Input data is split into fixed-size chunks (default 16 MB)
2. Each chunk is stored as a blob and its SHA-256 hash becomes a leaf reference
3. Leaf references are grouped (default: pairs) into tree nodes
4. Each node is JSON-encoded, signed with the private key, and stored as a blob
5. This process repeats up the tree until a single root node remains
6. The root hash is what you use to retrieve or verify the entire file

Node structure:
```json
{
  "is_root": true,
  "signed": "hex-encoded-signature",
  "children": ["sha256-abc...", "sha256-def..."]
}
```

## Thread Safety

Both `local.Storage` and `memory.Storage` are safe for concurrent use. The Merkle tree layer inherits thread safety from the underlying backend.

## License

MIT — see [LICENSE](LICENSE) for details.