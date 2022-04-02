package redis

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_parseDNS(t *testing.T) {
	tests := []struct {
		name        string
		dns         string
		expectError bool
		expectDSN   *dsn
	}{
		{
			name:        "simple",
			dns:         "redis://localhost:6379",
			expectError: false,
			expectDSN: &dsn{
				url: "redis://localhost:6379",
			},
		},
		{
			name:        "with user password",
			dns:         "redis://username:password@localhost:6379",
			expectError: false,
			expectDSN: &dsn{
				url: "redis://username:password@localhost:6379",
			},
		},
		{
			name:        "with compression",
			dns:         "redis://username:password@localhost:6379?compression=zstd&threshold=64",
			expectError: false,
			expectDSN: &dsn{
				url:         "redis://username:password@localhost:6379",
				compression: "zstd",
				threshold:   64,
			},
		},
		{
			name:        "Standalone full",
			dns:         "redis://username:password@localhost:6379/1?timeout=5s&compression=zstd&threshold=64",
			expectError: false,
			expectDSN: &dsn{
				url:         "redis://username:password@localhost:6379/1?timeout=5s",
				compression: "zstd",
				threshold:   64,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := newDSN(test.dns)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectDSN, dsn)
			}
		})
	}
}
