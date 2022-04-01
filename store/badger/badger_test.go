package badger

import (
	"context"
	"github.com/bitrainforest/kdb/store"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func makeStore(t *testing.T) store.Store {
	t.Helper()

	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	st, err := NewStore(dir)
	require.NoError(t, err)
	require.NotNil(t, st)
	return st
}

func TestNewStore(t *testing.T) {
	st := makeStore(t)
	require.NoError(t, st.Close())
}

func TestStore(t *testing.T) {

	st := makeStore(t)
	ctx := context.TODO()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	// Test Put

	err := st.Put(ctx, keys[0], values[0])

	require.NoError(t, err)

	// Test Flush
	v, err := st.Get(ctx, keys[0])
	require.ErrorIs(t, err, store.ErrNotFound)
	require.Nil(t, v)

	err = st.FlushPuts(ctx)
	require.NoError(t, err)

	v, err = st.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Equal(t, values[0], v)

	for i, k := range keys {
		err = st.Put(ctx, k, values[i])
		require.NoError(t, err)
	}
	err = st.FlushPuts(ctx)
	require.NoError(t, err)

	// Test BatchGet

	it := st.BatchGet(ctx, keys)
	require.NoError(t, it.Err())

	var vv [][]byte
	for it.Next() {
		vv = append(vv, it.Item().Value)
	}

	require.Equal(t, values, vv)

	prefix := []byte("key")

	it = st.Prefix(ctx, prefix, 3)

	var vv1 [][]byte
	for it.Next() {
		vv1 = append(vv1, it.Item().Value)
	}

	require.Equal(t, values, vv1)

	err = st.BatchDelete(ctx, keys)
	require.NoError(t, err)

	v, err = st.Get(ctx, keys[0])
	require.ErrorIs(t, err, store.ErrNotFound)
	require.Nil(t, v)

	require.NoError(t, st.Close())
}
