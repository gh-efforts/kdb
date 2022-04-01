package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeCompressor(t *testing.T, mode string) Compressor {
	t.Helper()

	c, err := NewCompressor(mode, 5)
	assert.NoError(t, err)

	return c
}

func TestZstdCompressor_Compress(t *testing.T) {

	cp := makeCompressor(t, "zstd")

	// Test compress
	data := []byte("hello world")
	out := cp.Compress(data)

	assert.NotNil(t, out)

	// Test decompress

	out2, err := cp.Decompress(out)
	assert.Nil(t, err)
	assert.Equal(t, data, out2)
}
