package store

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/iden3/go-merkletree-sql"
	"github.com/iden3/go-merkletree-sql/db/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDB = "/tmp/test-mt.db"

func TestAll(t *testing.T) {

	type testD struct {
		name string
		fn   func(t *testing.T, s *BoltStore)
	}
	tests := []testD{
		{"TestReturnKnownErrIfNotExists", func(t *testing.T, s *BoltStore) {
			test.TestReturnKnownErrIfNotExists(t, s)
		}},
		{"TestStorageInsertGet", func(t *testing.T, s *BoltStore) {
			test.TestStorageInsertGet(t, s)
		}},
		{"TestStorageWithPrefix", func(t *testing.T, s *BoltStore) {
			test.TestStorageWithPrefix(t, s)
		}},
		{"TestList", func(t *testing.T, s *BoltStore) {
			test.TestList(t, s)
		}},
		{"TestIterate", func(t *testing.T, s *BoltStore) {
			test.TestIterate(t, s)
		}},
		{"TestNewTree", func(t *testing.T, s *BoltStore) {
			test.TestNewTree(t, s)
		}},
		{"TestAddDifferentOrder", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestAddDifferentOrder(t, s, sb)
		}},
		{"TestAddRepeatedIndex", func(t *testing.T, s *BoltStore) {
			test.TestAddRepeatedIndex(t, s)
		}},
		{"TestGet", func(t *testing.T, s *BoltStore) {
			test.TestGet(t, s)
		}},
		{"TestUpdate", func(t *testing.T, s *BoltStore) {
			test.TestUpdate(t, s)
		}},
		{"TestUpdate2", func(t *testing.T, s *BoltStore) {
			test.TestUpdate2(t, s)
		}},
		{"TestGenerateAndVerifyProof128", func(t *testing.T, s *BoltStore) {
			test.TestGenerateAndVerifyProof128(t, s)
		}},
		{"TestTreeLimit", func(t *testing.T, s *BoltStore) {
			test.TestTreeLimit(t, s)
		}},
		{"TestSiblingsFromProof", func(t *testing.T, s *BoltStore) {
			test.TestSiblingsFromProof(t, s)
		}},
		{"TestVerifyProofCases", func(t *testing.T, s *BoltStore) {
			test.TestVerifyProofCases(t, s)
		}},
		{"TestVerifyProofFalse", func(t *testing.T, s *BoltStore) {
			test.TestVerifyProofFalse(t, s)
		}},
		{"TestGraphViz", func(t *testing.T, s *BoltStore) {
			test.TestGraphViz(t, s)
		}},
		{"TestDelete", func(t *testing.T, s *BoltStore) {
			test.TestDelete(t, s)
		}},
		{"TestDelete2", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestDelete2(t, s, sb)
		}},
		{"TestDelete3", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestDelete3(t, sb, s)
		}},
		{"TestDelete4", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestDelete4(t, s, sb)
		}},
		{"TestDelete5", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestDelete5(t, s, sb)
		}},
		{"TestDeleteNonExistingKeys", func(t *testing.T, s *BoltStore) {
			test.TestDeleteNonExistingKeys(t, s)
		}},
		{"TestDumpLeafsImportLeafs", func(t *testing.T, s *BoltStore) {
			var sb, shutdown = setupDB2(t)
			defer shutdown()
			test.TestDumpLeafsImportLeafs(t, s, sb)
		}},
		{"TestAddAndGetCircomProof", func(t *testing.T, s *BoltStore) {
			test.TestAddAndGetCircomProof(t, s)
		}},
		{"TestUpdateCircomProcessorProof", func(t *testing.T, s *BoltStore) {
			test.TestUpdateCircomProcessorProof(t, s)
		}},
		{"TestSmtVerifier", func(t *testing.T, s *BoltStore) {
			test.TestSmtVerifier(t, s)
		}},
		{"TestTypesMarshalers", func(t *testing.T, s *BoltStore) {
			test.TestTypesMarshalers(t, s)
		}},
	}

	for _, tt := range tests {
		func() {
			var sb, shutdown = setupDB(t)
			defer shutdown()
			t.Run(tt.name, func(t *testing.T) {
				tt.fn(t, sb)
			})
		}()
	}
}

func TestPutGet(t *testing.T) {
	var b, shutdown = setupDB(t)
	defer shutdown()

	k, err := merkletree.NewHashFromBigInt(big.NewInt(1))
	assert.NoError(t, err)
	v, err := merkletree.NewHashFromBigInt(big.NewInt(1))
	assert.NoError(t, err)

	n := merkletree.NewNodeLeaf(k, v)

	err = b.Put(context.Background(), k.BigInt().Bytes(), n)
	assert.NoError(t, err)

	rn, err := b.Get(context.Background(), k.BigInt().Bytes())
	assert.NoError(t, err)

	assert.Equal(t, n, rn)
}

func TestSetRoot(t *testing.T) {
	var b, shutdown = setupDB(t)
	defer shutdown()

	r, err := merkletree.NewHashFromBigInt(big.NewInt(1))
	assert.NoError(t, err)
	b.SetRoot(context.Background(), r)

	res, err := b.GetRoot(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, r, res)
}

func TestLoadRoot(t *testing.T) {
	_ = os.Remove(testDB)
	b, err := bolt.Open(testDB, 0600, nil)
	assert.NoError(t, err)

	defer func() {
		require.NoError(t, b.Close())
		_ = os.Remove(testDB)
	}()

	root, err := merkletree.NewHashFromBigInt(big.NewInt(22))
	assert.NoError(t, err)

	b.Update(func(tx *bolt.Tx) error {
		bu, err := tx.CreateBucketIfNotExists([]byte("tree"))
		assert.NoError(t, err)
		return bu.Put([]byte("root"), []byte(root.Hex()))
	})

	s, err := NewBoltStorage(b)
	assert.NoError(t, err)
	res, err := s.GetRoot(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, root, res)
}

func TestList(t *testing.T) {
	var b, shutdown = setupDB(t)
	defer shutdown()

	k, err := merkletree.NewHashFromBigInt(big.NewInt(1))
	assert.NoError(t, err)
	v, err := merkletree.NewHashFromBigInt(big.NewInt(1))
	assert.NoError(t, err)

	n := merkletree.NewNodeLeaf(k, v)

	err = b.Put(context.Background(), k.BigInt().Bytes(), n)
	assert.NoError(t, err)

	// put another node
	k, err = merkletree.NewHashFromBigInt(big.NewInt(2))
	assert.NoError(t, err)

	v, err = merkletree.NewHashFromBigInt(big.NewInt(2))
	assert.NoError(t, err)

	n = merkletree.NewNodeLeaf(k, v)
	err = b.Put(context.Background(), k.BigInt().Bytes(), n)
	assert.NoError(t, err)

	l, err := b.List(context.Background(), 0)
	assert.NoError(t, err)

	fmt.Printf("L: %+v", l)

}

// setup test bolt db
func setupDB(t *testing.T) (s *BoltStore, shutdown func()) {
	_ = os.Remove(testDB)

	b, err := bolt.Open(testDB, 0600, &bolt.Options{Timeout: 5 * time.Second})
	assert.NoError(t, err)

	s, err = NewBoltStorage(b)
	assert.NoError(t, err)

	shutdown = func() {
		require.NoError(t, b.Close())
		_ = os.Remove(testDB)
	}
	return s, shutdown
}

func setupDB2(t *testing.T) (s *BoltStore, shutdown func()) {
	var testDB = "/tmp/testdb2.db"
	_ = os.Remove(testDB)

	b, err := bolt.Open(testDB, 0600, &bolt.Options{Timeout: 1 * time.Second})
	assert.NoError(t, err)

	s, err = NewBoltStorage(b)
	assert.NoError(t, err)

	shutdown = func() {
		require.NoError(t, b.Close())
		_ = os.Remove(testDB)
	}
	return s, shutdown
}

func TestProof(t *testing.T) {
	var b, shutdown = setupDB(t)
	defer shutdown()

	tree, err := merkletree.NewMerkleTree(context.Background(), b, 10)
	assert.NoError(t, err)

	tree.Add(context.Background(), big.NewInt(0), big.NewInt(1))

	proof, _, err := tree.GenerateProof(context.Background(), big.NewInt(1), nil)
	assert.NoError(t, err)
	fmt.Println(proof)

	proof0, _, err := tree.GenerateProof(context.Background(), big.NewInt(0), nil)
	assert.NoError(t, err)
	fmt.Println(proof0)

}
