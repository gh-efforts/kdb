package store

import (
	"encoding/hex"
	"strconv"
)

type KV struct {
	Key, Value []byte
}

func (kv *KV) Size() int {
	if kv == nil {
		return 0
	}

	return len(kv.Key) + len(kv.Value)
}

type Key []byte

func (k Key) String() string {
	return hex.EncodeToString(k)
}

type Limit int

func (l Limit) Reached(count uint64) bool {
	return l.Bounded() && count >= uint64(l)
}

func (l Limit) Bounded() bool {
	return int(l) > 0
}

func (l Limit) Unbounded() bool {
	return int(l) <= 0
}

func (l Limit) String() string {
	if l.Unbounded() {
		return "unlimited"
	}

	return strconv.FormatInt(int64(l), 10)
}
