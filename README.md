# smt-bolt - Bolt db storage for https://github.com/iden3/go-merkletree-sql

## Install

`go get -u github.com/demonsh/smt-bolt`

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"time"

	store "github.com/demonsh/smt-bolt"
	"github.com/iden3/go-merkletree-sql"
	bolt "go.etcd.io/bbolt"
)

var db = "./mt.db"

func main() {

	b, err := bolt.Open(db, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	s, err := store.NewBoltStorage(b)
	if err != nil {
		log.Fatal(err)
	}

	mt, err := merkletree.NewMerkleTree(context.Background(), s, 10)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	mt.Add(ctx, big.NewInt(1), big.NewInt(1))
	mt.Update(ctx, big.NewInt(1), big.NewInt(2))
	proof, value, err := mt.GenerateProof(ctx, big.NewInt(1), nil)
	fmt.Println(value)
	fmt.Println(proof)
}
```