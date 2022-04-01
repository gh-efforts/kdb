package badger

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"net/url"
	"strings"
)

type dsn struct {
	dbPath      string
	compression options.CompressionType // none, snappy, zstd
}

func newDSN(dsnString string) (*dsn, error) {
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse badger dsn %q: %w", dsnString, err)
	}

	var paths []string
	if u.Hostname() != "" {
		paths = append(paths, u.Hostname())
	}

	if u.Path != "" {
		paths = append(paths, u.Path)
	}

	r := &dsn{
		dbPath: strings.Join(paths, "/"),
	}

	switch u.Query().Get("compression") {
	case "snappy":
		r.compression = options.Snappy
	case "zstd":
		r.compression = options.ZSTD
	case "none", "":
		r.compression = options.None
	default:
		return nil, fmt.Errorf("badger: invalid compression type %q", u.Query().Get("compression"))
	}
	return r, nil
}

func dsnToOptions(d *dsn) badger.Options {
	opts := badger.DefaultOptions(d.dbPath).WithLogger(nil).WithCompression(d.compression)
	return opts
}
