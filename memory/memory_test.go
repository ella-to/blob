package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"ella.to/blob"
)

func TestMemoryStorage_Put(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		expectError   bool
		expectedError error
	}{
		{
			name:          "empty data",
			data:          []byte{},
			expectError:   true,
			expectedError: io.EOF,
		},
		{
			name:        "small data",
			data:        []byte("hello world"),
			expectError: false,
		},
		{
			name:        "medium data",
			data:        bytes.Repeat([]byte("test"), 1000),
			expectError: false,
		},
		{
			name:        "large data",
			data:        bytes.Repeat([]byte("x"), 10*1024*1024), // 10MB
			expectError: false,
		},
		{
			name:        "single byte",
			data:        []byte("a"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := New()
			ctx := context.Background()

			ref, size, err := storage.Put(ctx, bytes.NewReader(tt.data))

			if tt.expectError {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tt.expectedError)
				assert.Nil(t, ref)
				assert.Equal(t, int64(0), size)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ref)
				assert.Equal(t, int64(len(tt.data)), size)
			}
		})
	}
}

func TestMemoryStorage_Get(t *testing.T) {
	storage := New()
	ctx := context.Background()

	// Put some data
	testData := []byte("test data for retrieval")
	ref, _, err := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err)

	t.Run("get existing data", func(t *testing.T) {
		rc, err := storage.Get(ctx, ref)
		require.NoError(t, err)
		defer rc.Close()

		data, err := io.ReadAll(rc)
		assert.NoError(t, err)
		assert.Equal(t, testData, data)
	})

	t.Run("get non-existent data", func(t *testing.T) {
		fakeRef := []byte("sha256-0000000000000000000000000000000000000000000000000000000000000000")
		rc, err := storage.Get(ctx, fakeRef)
		assert.Error(t, err)
		assert.ErrorIs(t, err, blob.ErrNotFound)
		assert.Nil(t, rc)
	})
}

func TestMemoryStorage_List(t *testing.T) {
	storage := New()
	ctx := context.Background()

	t.Run("empty storage", func(t *testing.T) {
		count := 0
		for ref, err := range storage.List(ctx) {
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, ref)
			count++
		}
		assert.Equal(t, 0, count)
	})

	t.Run("multiple items", func(t *testing.T) {
		// Add multiple items
		refs := make(map[string]bool)
		for i := 0; i < 10; i++ {
			data := []byte{byte(i)}
			ref, _, err := storage.Put(ctx, bytes.NewReader(data))
			require.NoError(t, err)
			refs[ref.String()] = true
		}

		// List and verify
		count := 0
		for ref, err := range storage.List(ctx) {
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
			assert.NotNil(t, ref)
			assert.True(t, refs[ref.String()], "unexpected ref: %s", ref.String())
			count++
		}
		assert.Equal(t, len(refs), count)
	})
}

func TestMemoryStorage_PutIdempotent(t *testing.T) {
	storage := New()
	ctx := context.Background()

	testData := []byte("idempotent test")

	// Put the same data twice
	ref1, size1, err1 := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err1)

	ref2, size2, err2 := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err2)

	// Should return the same reference
	assert.Equal(t, ref1.String(), ref2.String())
	assert.Equal(t, size1, size2)
}

func TestMemoryStorage_Concurrent(t *testing.T) {
	storage := New()
	ctx := context.Background()

	const numGoroutines = 100
	const numOperations = 10

	var wg sync.WaitGroup
	var mu sync.Mutex
	refs := make([][]byte, 0, numGoroutines*numOperations)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Create unique data for each operation to get unique hashes
				data := []byte(fmt.Sprintf("data-%d-%d", id, j))
				ref, _, err := storage.Put(ctx, bytes.NewReader(data))
				assert.NoError(t, err)
				if err == nil {
					mu.Lock()
					refs = append(refs, ref)
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all written data can be read
	for _, ref := range refs {
		rc, err := storage.Get(ctx, ref)
		assert.NoError(t, err)
		if rc != nil {
			rc.Close()
		}
	}
}

func TestMemoryStorage_GetMultipleTimes(t *testing.T) {
	storage := New()
	ctx := context.Background()

	testData := []byte("test data for multiple reads")
	ref, _, err := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err)

	// Read the same data multiple times
	for i := 0; i < 10; i++ {
		rc, err := storage.Get(ctx, ref)
		require.NoError(t, err)

		data, err := io.ReadAll(rc)
		assert.NoError(t, err)
		assert.Equal(t, testData, data)
		rc.Close()
	}
}

func TestMemoryStorage_ContextCancellation(t *testing.T) {
	storage := New()

	t.Run("cancelled context on Put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		data := []byte("test data")
		_, _, err := storage.Put(ctx, bytes.NewReader(data))
		// Memory storage doesn't check context in Put, so this might not error
		// but we're testing that it doesn't panic
		_ = err
	})

	t.Run("cancelled context on List", func(t *testing.T) {
		// Add some data first
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			storage.Put(ctx, bytes.NewReader([]byte{byte(i)}))
		}

		// Now try to list with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		count := 0
		for range storage.List(cancelCtx) {
			count++
			if count > 10 {
				break // Prevent infinite loop
			}
		}
	})
}
