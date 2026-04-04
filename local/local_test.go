package local

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"ella.to/blob"
)

func TestLocalStorage_Put(t *testing.T) {
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
			tmpDir := t.TempDir()
			storage := NewStorage(WithPath(tmpDir))
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

				// Verify file was created with correct name
				filePath := filepath.Join(tmpDir, ref.String())
				_, err := os.Stat(filePath)
				assert.NoError(t, err, "file should exist")
			}
		})
	}
}

func TestLocalStorage_Get(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
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
		// Create a valid hash format that doesn't exist in storage
		fakeRef := make([]byte, 32) // 32 bytes for SHA-256
		for i := range fakeRef {
			fakeRef[i] = 0xFF // Fill with non-zero values
		}
		rc, err := storage.Get(ctx, fakeRef)
		assert.Error(t, err)
		assert.ErrorIs(t, err, blob.ErrNotFound)
		assert.Nil(t, rc)
	})
}

func TestLocalStorage_List(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	t.Run("empty storage", func(t *testing.T) {
		count := 0
		for ref, err := range storage.List(ctx) {
			if ref == nil && err == nil {
				break
			}
			assert.NoError(t, err)
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
			if ref == nil && err == nil {
				break
			}
			assert.NoError(t, err)
			if ref != nil {
				assert.True(t, refs[ref.String()], "unexpected ref: %s", ref.String())
				count++
			}
		}
		assert.Equal(t, len(refs), count)
	})
}

func TestLocalStorage_PutIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	testData := []byte("idempotent test")

	// Put the same data twice
	ref1, size1, err1 := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err1)

	ref2, size2, err2 := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err2)

	// Should return the same reference (same hash)
	assert.Equal(t, ref1.String(), ref2.String())
	assert.Equal(t, size1, size2)

	// Should only have one file
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)

	actualFiles := 0
	for _, f := range files {
		if !f.IsDir() {
			actualFiles++
		}
	}
	assert.Equal(t, 1, actualFiles, "should only have one file stored")
}

func TestLocalStorage_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	const numGoroutines = 50
	const numOperations = 5

	var wg sync.WaitGroup
	var mu sync.Mutex
	refs := make([][]byte, 0, numGoroutines*numOperations)
	errors := make([]error, 0)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				// Create unique data for each operation
				data := []byte(fmt.Sprintf("data-%d-%d", id, j))
				ref, _, err := storage.Put(ctx, bytes.NewReader(data))
				mu.Lock()
				if err != nil {
					errors = append(errors, err)
				} else {
					refs = append(refs, ref)
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		assert.NoError(t, err)
	}

	// Verify all written data can be read
	for _, ref := range refs {
		rc, err := storage.Get(ctx, ref)
		assert.NoError(t, err)
		if rc != nil {
			rc.Close()
		}
	}
}

func TestLocalStorage_GetMultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
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

func TestLocalStorage_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))

	t.Run("cancelled context on List", func(t *testing.T) {
		// Add some data first
		ctx := context.Background()
		for i := range 5 {
			_, _, _ = storage.Put(ctx, bytes.NewReader([]byte{byte(i)}))
		}

		// Now try to list with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		count := 0
		for _, err := range storage.List(cancelCtx) {
			if err != nil {
				assert.ErrorIs(t, err, context.Canceled)
				break
			}
			count++
			if count > 10 {
				break // Prevent infinite loop
			}
		}
	})
}

func TestLocalStorage_TempFileCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	// Test that empty data doesn't leave temp files
	_, _, err := storage.Put(ctx, bytes.NewReader([]byte{}))
	assert.Error(t, err)

	// Check no temp files remain
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)

	for _, f := range files {
		assert.False(t, !f.IsDir() && len(f.Name()) > 4 && f.Name()[:4] == "tmp-",
			"temp file should be cleaned up: %s", f.Name())
	}
}

func TestLocalStorage_FileIntegrity(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir))
	ctx := context.Background()

	testData := []byte("test data for integrity check")
	ref, _, err := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err)

	// Read file content directly and verify it matches
	filePath := filepath.Join(tmpDir, ref.String())
	fileData, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, testData, fileData)

	// Also verify through Get
	rc, err := storage.Get(ctx, ref)
	require.NoError(t, err)
	defer rc.Close()

	getData, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, testData, getData)
}

func TestLocalStorage_EncryptedPutGet(t *testing.T) {
	tmpDir := t.TempDir()
	storage := NewStorage(WithPath(tmpDir), WithKey("my-secret-key"))
	ctx := context.Background()

	testData := []byte("sensitive local storage payload")
	ref, _, err := storage.Put(ctx, bytes.NewReader(testData))
	require.NoError(t, err)

	storedData, err := os.ReadFile(filepath.Join(tmpDir, ref.String()))
	require.NoError(t, err)
	assert.NotEqual(t, testData, storedData)

	rc, err := storage.Get(ctx, ref)
	require.NoError(t, err)
	defer rc.Close()

	decryptedData, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, testData, decryptedData)
}
