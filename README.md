# kdb

[![Build and test](https://github.com/bitrainforest/kdb/actions/workflows/build.yml/badge.svg)](https://github.com/bitrainforest/kdb/actions/workflows/build.yml)
[![Lint Code Base](https://github.com/bitrainforest/kdb/actions/workflows/linter.yml/badge.svg)](https://github.com/bitrainforest/kdb/actions/workflows/linter.yml)

Distributed Key/Value Store Abstraction Library written in Go. 

Inspired by [streamingfast/kvdb](https://github.com/streamingfast/kvdb).

Stage: Alpha

## Usage

[example](example/main.go)

## Backends

| name     | dns                                                                                    | Description                                                                    | Example                                                                                                                                     |
|----------|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `badger` | `badger:///Users/john/kdb/badger-db.db?compression=zstd`                               | compression: `snappy`, `zstd`, `none`                                          | [example](store/badger/dsn_test.go)                                                                                                         |
| `etcd`   | `etcd://username:password@localhost:2379,localhost:2380?compression=zstd&threshold=64` | compression: `zstd`, `none`  <br/> threshold: `compression threshold in bytes` | [example](store/etcd/dsn_test.go)                                                                                                           |
| `redis`  | `redis://username:password@localhost:6379/1?timeout=5s&compression=zstd&threshold=64`  | compression: `zstd`, `none`  <br/> threshold: `compression threshold in bytes` | [example1](store/etcd/dsn_test.go), [example2](https://github.com/lettuce-io/lettuce-core/wiki/Redis-URI-and-connection-details#uri-syntax) |

## Tests

```shell
go test ./...
