# Valkeyrie Sqlite3

[![GoDoc](https://godoc.org/github.com/ozanturksever/sqlite3kv?status.png)](https://godoc.org/github.com/ozanturksever/sqlite3kv)
[![Build Status](https://github.com/ozanturksever/sqlite3kv/actions/workflows/build.yml/badge.svg)](https://github.com/ozanturksever/sqlite3kv/actions/workflows/build.yml)

[`valkeyrie`](https://github.com/kvtools/valkeyrie) provides a Go native library to store metadata using Distributed Key/Value stores (or common databases).

## Compatibility

A **storage backend** in `valkeyrie` implements (fully or partially) the [Store](https://github.com/kvtools/valkeyrie/blob/master/store/store.go#L69) interface.

| Calls                 | SqliteV3 |
|-----------------------|:--------:|
| Put                   |   🟢️    |
| Get                   |   🟢️    |
| Delete                |   🟢️    |
| Exists                |   🟢️    |
| Watch                 |   🟢️    |
| WatchTree             |   🟢️    |
| NewLock (Lock/Unlock) |    ️     |
| List                  |   🟢️    |
| DeleteTree            |   🟢️    |
| AtomicPut             |   🟢️    |
| AtomicDelete          |   🟢️    |

## Supported Versions

## Examples

```go
package main

import (
	"context"
	"log"

	"github.com/kvtools/valkeyrie"
	"github.com/ozanturksever/sqlite3kv"
)

func main() {
	ctx := context.Background()

	config := &sqlite3kv.Config{}

	kv, err := valkeyrie.NewStore(ctx, sqlite3kv.StoreName, []string{ "/tmp/test.db"}, config)
	if err != nil {
		log.Fatal("Cannot create store")
	}

	key := "foo"

	err = kv.Put(ctx, key, []byte("bar"), nil)
	if err != nil {
		log.Fatalf("Error trying to put value at key: %v", key)
	}

	pair, err := kv.Get(ctx, key, nil)
	if err != nil {
		log.Fatalf("Error trying accessing value at key: %v", key)
	}

	log.Printf("value: %s", string(pair.Value))

	err = kv.Delete(ctx, key)
	if err != nil {
		log.Fatalf("Error trying to delete key %v", key)
	}
}
```
