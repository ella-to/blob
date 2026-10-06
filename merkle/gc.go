package merkle

import (
	"context"
	"errors"
	"fmt"
	"io"

	"ella.to/blob"
)

var (
	ErrNotRoot            = errors.New("not a root node")
	ErrDeleteNotSupported = errors.New("storage does not support delete")
	ErrPrivateKeyRequired = errors.New("private key is required")
)

// Delete marks the tree under root as deleted by storing a signed tombstone.
// It is cheap and nothing is removed yet: the root disappears from
// ListRootNodes, and GC reclaims every blob that no other live tree uses.
// Until GC runs, putting the same content again revives the root.
func (m *Storage) Delete(ctx context.Context, root blob.Ref) error {
	if m.privateKey == nil {
		return ErrPrivateKeyRequired
	}

	m.gcMu.RLock()
	defer m.gcMu.RUnlock()

	node, err := m.isValidMerkleNode(ctx, root)
	if errors.Is(err, ErrNotNode) {
		return fmt.Errorf("%w: %s", ErrNotRoot, root)
	} else if err != nil {
		return err
	}

	if !node.IsRoot {
		return fmt.Errorf("%w: %s", ErrNotRoot, root)
	}

	_, _, err = m.storage.Put(ctx, SignTombstoneReader(root, m.privateKey))
	return err
}

// GCStats reports what a GC run did.
type GCStats struct {
	Roots      int // live roots
	Tombstones int // deleted roots processed
	Deleted    int // blobs removed, including roots and tombstones
}

// GC removes the trees of deleted roots. A blob is only removed when it is not
// reachable from any live root, since chunks and nodes are shared between
// trees with common content. Put and Delete are blocked while GC runs.
//
// Blobs are removed bottom up and the tombstone last, so an interrupted GC
// is simply resumed by the next run.
func (m *Storage) GC(ctx context.Context) (GCStats, error) {
	var stats GCStats

	deleter, ok := m.storage.(blob.Deleter)
	if !ok {
		return stats, ErrDeleteNotSupported
	}

	m.gcMu.Lock()
	defer m.gcMu.Unlock()

	roots, tombstones, err := m.scan(ctx)
	if err != nil {
		return stats, err
	}

	// mark
	live := make(map[string]struct{})
	for _, root := range roots {
		if _, ok := tombstones[string(root)]; ok {
			continue
		}

		stats.Roots++
		err := m.walk(ctx, root, false, func(ref blob.Ref) bool {
			if _, ok := live[string(ref)]; ok {
				return false
			}
			live[string(ref)] = struct{}{}
			return true
		})
		if err != nil {
			return stats, err
		}
	}

	// collect garbage of all deleted trees before removing anything, as
	// deleted trees can share blobs as well
	garbage := make([]blob.Ref, 0)
	seen := make(map[string]struct{})
	for rootKey := range tombstones {
		err := m.walk(ctx, blob.Ref(rootKey), true, func(ref blob.Ref) bool {
			if _, ok := live[string(ref)]; ok {
				return false
			}
			if _, ok := seen[string(ref)]; ok {
				return false
			}
			seen[string(ref)] = struct{}{}
			garbage = append(garbage, ref)
			return true
		})
		// a previous GC may have already removed part of the tree
		if err != nil {
			return stats, err
		}
	}

	// sweep, children first: walk is pre-order, so reverse it
	for i := len(garbage) - 1; i >= 0; i-- {
		if err := deleter.Delete(ctx, garbage[i]); err != nil {
			return stats, err
		}
		stats.Deleted++
	}

	for _, tombstone := range tombstones {
		if err := deleter.Delete(ctx, tombstone); err != nil {
			return stats, err
		}
		stats.Tombstones++
		stats.Deleted++
	}

	return stats, nil
}

// scan lists every root and every tombstone in the storage. Tombstones are
// keyed by the root they delete.
func (m *Storage) scan(ctx context.Context) (roots []blob.Ref, tombstones map[string]blob.Ref, err error) {
	tombstones = make(map[string]blob.Ref)

	for ref, err := range m.storage.List(ctx) {
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}

		if ref == nil {
			continue
		}

		obj, err := m.open(ctx, ref)
		if errors.Is(err, ErrInvalidNode) || errors.Is(err, blob.ErrNotFound) {
			// signed by another key, or removed since it was listed
			continue
		} else if err != nil {
			return nil, nil, err
		}
		obj.close()

		switch {
		case obj.node != nil && obj.node.IsRoot:
			roots = append(roots, ref)
		case obj.tombstone != nil:
			tombstones[string(obj.tombstone.Root)] = ref
		}
	}

	return roots, tombstones, nil
}

// walk visits ref and everything below it in pre-order. Returning false from
// fn skips the children of ref. With skipMissing, blobs that are already
// gone are ignored instead of failing the walk.
func (m *Storage) walk(ctx context.Context, ref blob.Ref, skipMissing bool, fn func(blob.Ref) bool) error {
	if !fn(ref) {
		return nil
	}

	obj, err := m.open(ctx, ref)
	if skipMissing && errors.Is(err, blob.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	obj.close()

	if obj.node == nil {
		return nil
	}

	for _, child := range obj.node.Children {
		if err := m.walk(ctx, child, skipMissing, fn); err != nil {
			return err
		}
	}

	return nil
}
