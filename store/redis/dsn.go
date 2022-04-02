package redis

import (
	"fmt"
	"net/url"
	"strconv"
)

type dsn struct {
	url         string // https://github.com/lettuce-io/lettuce-core/wiki/Redis-URI-and-connection-details
	compression string // none, zstd
	threshold   int    // compression threshold in bytes
}

func newDSN(dsnString string) (*dsn, error) {

	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse redis dsn %q: %w", dsnString, err)
	}

	query := u.Query()

	d := &dsn{}

	if query.Has("compression") {
		d.compression = query.Get("compression")
		query.Del("compression")
	}

	if query.Has("threshold") {
		threshold := query.Get("threshold")
		if threshold == "0" {
			d.threshold = 0
		} else {
			i, err := strconv.Atoi(threshold)
			if err != nil {
				return nil, fmt.Errorf("cannot parse compression threshold %q: %w", threshold, err)
			}
			d.threshold = i
		}
		query.Del("threshold")
	}

	u.RawQuery = query.Encode()

	d.url = u.String()
	return d, nil
}
