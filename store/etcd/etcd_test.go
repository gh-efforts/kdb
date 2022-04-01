package etcd

import (
	"context"
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

func TestNewStore(t *testing.T) {
	st := makeStore(t)
	require.NotNil(t, st)
}

func TestStore(t *testing.T) {
	st := makeStore(t)
	require.NotNil(t, st)
	ctx := context.TODO()
	key := []byte("test_key")
	value := []byte("test_value")
	err := st.Put(ctx, key, value)
	require.NoError(t, err)

	v, err := st.Get(ctx, key)
	t.Log(v, err)
	require.ErrorIs(t, err, store.ErrNotFound)
	require.Nil(t, v)

	err = st.FlushPuts(ctx)
	require.NoError(t, err)

	v, err = st.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, v)

	err = st.Delete(ctx, key)
	require.NoError(t, err)
}

func TestStore_Batch(t *testing.T) {
	st := makeStore(t)
	require.NotNil(t, st)

	ctx := context.TODO()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	for i, key := range keys {
		err := st.Put(ctx, key, values[i])
		require.NoError(t, err)
	}

	v, err := st.Get(ctx, keys[0])
	require.ErrorIs(t, err, store.ErrNotFound)
	require.Nil(t, v)

	err = st.FlushPuts(ctx)
	require.NoError(t, err)

	it := st.BatchGet(ctx, keys)
	var vv [][]byte
	for it.Next() {
		vv = append(vv, it.Item().Value)
	}
	require.NoError(t, it.Err())
	require.Equal(t, values, vv)

	err = st.BatchDelete(ctx, keys)
	require.NoError(t, err)

	it = st.BatchGet(ctx, keys)
	require.Equal(t, false, it.Next())
	require.ErrorIs(t, it.Err(), store.ErrNotFound)
}
