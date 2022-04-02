# kdb

[![Lint Code Base](https://github.com/bitrainforest/kdb/actions/workflows/linter.yml/badge.svg)](https://github.com/bitrainforest/kdb/actions/workflows/linter.yml)

Distributed key-value Store Abstraction Library written in Go.

Inspired by [streamingfast/kvdb](https://github.com/streamingfast/kvdb), used by [BitRainforest](https://github.com/bitrainforest).

Stage: Alpha

## Usage

[example](example/main.go)

## Backends

### badger

* dsn: `badger:///Users/john/kdb/badger-db.db?compression=zstd`
* compression: `snappy`, `zstd`, `none`
* example: [store/badger/dsn_test.go](store/badger/dsn_test.go)

## etcd

* dns: `etcd://username:password@localhost:2379,localhost:2380?compression=zstd&threshold=64`
* compression: `zstd`, `none`
* threshold: compression threshold in bytes
* example: [store/etcd/dsn_test.go](store/etcd/dsn_test.go)

## redis

* dsn: `redis://username:password@localhost:6379/1?timeout=5s&compression=zstd&threshold=64`
* compression: `zstd`, `none`
* threshold: compression threshold in bytes
* example: [store/redis/dsn_test.go](store/redis/dsn_test.go), [Redis-URI](https://github.com/lettuce-io/lettuce-core/wiki/Redis-URI-and-connection-details#uri-syntax)

## Tests

```shell
go test ./...
