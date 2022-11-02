package store

import (
	"bytes"
	"context"
	"fmt"

	"github.com/iden3/go-merkletree-sql"
	bolt "go.etcd.io/bbolt"
)

// BoltStore implements the db.BoltStore interface
type BoltStore struct {
	prefix      []byte
	db          *bolt.DB
	currentRoot *merkletree.Hash
}

// NewMemoryBoldStor returns a new BoltStore
func NewBoltStorage(db *bolt.DB) (*BoltStore, error) {

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("tree"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &BoltStore{[]byte{}, db, nil}, nil
}

// WithPrefix implements the method WithPrefix of the interface db.Storage
func (m *BoltStore) WithPrefix(prefix []byte) merkletree.Storage {
	return &BoltStore{merkletree.Concat(m.prefix, prefix), m.db, nil}
}

// Get retrieves a value from a key in the db.Storage
func (m *BoltStore) Get(_ context.Context, key []byte) (*merkletree.Node, error) {

	var v []byte

	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tree"))
		v = b.Get(merkletree.Concat(m.prefix, key[:]))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, merkletree.ErrNotFound
	}

	return merkletree.NewNodeFromBytes(v[:])
}

// Put inserts new node into merkletree
func (m *BoltStore) Put(_ context.Context, key []byte,
	node *merkletree.Node) error {

	v := node.Value()

	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tree"))
		err := b.Put(merkletree.Concat(m.prefix, key), v)
		return err
	})
}

// GetRoot returns current merkletree root
func (m *BoltStore) GetRoot(_ context.Context) (*merkletree.Hash, error) {
	if m.currentRoot != nil {
		hash := merkletree.Hash{}
		copy(hash[:], m.currentRoot[:])
		return &hash, nil
	}

	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tree"))
		v := b.Get([]byte("root"))
		if v == nil {
			return merkletree.ErrNotFound
		}

		hash, err := merkletree.NewHashFromHex(string(v))
		if err != nil {
			return err
		}
		m.currentRoot = hash

		return nil
	})

	return m.currentRoot, err
}

// SetRoot updates current merkletree root
func (m *BoltStore) SetRoot(_ context.Context, hash *merkletree.Hash) error {
	root := &merkletree.Hash{}
	copy(root[:], hash[:])

	err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tree"))
		return b.Put([]byte("root"), []byte(root.Hex()))
	})

	if err != nil {
		return err
	}

	m.currentRoot = root

	return nil
}

// Iterate implements the method Iterate of the interface db.Storage
func (m *BoltStore) Iterate(_ context.Context,
	f func([]byte, *merkletree.Node) (bool, error)) error {

	return m.db.View(func(tx *bolt.Tx) error {

		c := tx.Bucket([]byte("tree")).Cursor()

		for k, v := c.Seek(m.prefix); k != nil && bytes.HasPrefix(k, m.prefix); k, v = c.Next() {
			n, err := merkletree.NewNodeFromBytes(v[:])
			if err != nil {
				return err
			}

			cont, err := f(merkletree.Clone(bytes.TrimPrefix(k, m.prefix)), n)
			if err != nil {
				return err
			}
			if !cont {
				break
			}
		}

		return nil
	})
}

// List implements the method List of the interface db.Storage
func (m *BoltStore) List(_ context.Context, limit int) ([]merkletree.KV, error) {

	var res []merkletree.KV

	err := m.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("tree")).Cursor()

		count := 0
		for k, v := c.Seek(m.prefix); k != nil && bytes.HasPrefix(k, m.prefix); k, v = c.Next() {
			n, err := merkletree.NewNodeFromBytes(v[:])
			if err != nil {
				return err
			}

			res = append(res, merkletree.KV{K: merkletree.Clone(bytes.TrimPrefix(k, m.prefix)), V: *n})
			count++
			if limit != 0 && count >= limit {
				break
			}
		}

		return nil
	})
	return res, err
}
