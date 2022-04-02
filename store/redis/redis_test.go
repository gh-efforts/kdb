package redis

import (
	"github.com/bitrainforest/kdb/store"
	"github.com/stretchr/testify/require"
	"testing"
)

func makeStore(t *testing.T) store.Store {
	t.Helper()

	dsn := "etcd://localhost:2379"
	st, err := NewStore(dsn)

	require.NoError(t, err)
	require.NotNil(t, st)
	return st
}
