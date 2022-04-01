package etcd

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type dsn struct {
	endpoints   []string
	username    string
	password    string
	compression string // none, zstd
	threshold   int    // compression threshold in bytes
}

func newDSN(dsnString string) (*dsn, error) {
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse etcd dsn %q: %w", dsnString, err)
	}
	d := &dsn{}

	d.endpoints = append(d.endpoints, strings.Split(u.Host, ",")...)

	if u.User != nil {
		d.username = u.User.Username()
		d.password, _ = u.User.Password()
	}
	if u.Query().Has("compression") {
		d.compression = u.Query().Get("compression")
	}
	if u.Query().Has("threshold") {
		threshold := u.Query().Get("threshold")
		v, err := strconv.ParseInt(threshold, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse compression threshold: %w", err)
		}
		d.threshold = int(v)
	}
	return d, nil
}
