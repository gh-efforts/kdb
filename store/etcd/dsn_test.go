package etcd

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
			dns:         "etcd://localhost:2379",
			expectError: false,
			expectDSN: &dsn{
				endpoints: []string{"localhost:2379"},
			},
		},
		{
			name:        "with user password",
			dns:         "etcd://username:password@localhost:2379",
			expectError: false,
			expectDSN: &dsn{
				endpoints: []string{"localhost:2379"},
				username:  "username",
				password:  "password",
			},
		},
		{
			name:        "with user password",
			dns:         "etcd://username:password@localhost:2379?compression=zstd&threshold=64",
			expectError: false,
			expectDSN: &dsn{
				endpoints:   []string{"localhost:2379"},
				username:    "username",
				password:    "password",
				compression: "zstd",
				threshold:   64,
			},
		},
		{
			name:        "multiple endpoints",
			dns:         "etcd://username:password@localhost:2379,localhost:2380?compression=zstd&threshold=64",
			expectError: false,
			expectDSN: &dsn{
				endpoints:   []string{"localhost:2379", "localhost:2380"},
				username:    "username",
				password:    "password",
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
