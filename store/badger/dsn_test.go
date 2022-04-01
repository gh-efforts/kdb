package badger

import (
	"github.com/dgraph-io/badger/v3/options"
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
			name:        "local dir",
			dns:         "badger://badger-db.db?compression=snappy",
			expectError: false,
			expectDSN: &dsn{
				dbPath:      "badger-db.db",
				compression: options.Snappy,
			},
		},
		{
			name:        "absolute path",
			dns:         "badger:///Users/john/kdb/badger-db.db?compression=zstd",
			expectError: false,
			expectDSN: &dsn{
				dbPath:      "/Users/john/kdb/badger-db.db",
				compression: options.ZSTD,
			},
		},
		{
			name:        "none compression",
			dns:         "badger:///Users/john/kdb/badger-db.db",
			expectError: false,
			expectDSN: &dsn{
				dbPath:      "/Users/john/kdb/badger-db.db",
				compression: options.None,
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
