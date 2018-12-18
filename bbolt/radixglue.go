/*
Copyright (c) 2018 Simon Schmidt
Copyright (c) 2013-2018 coreos/etcd.io Authors
Copyright (c) 2013 Ben Johnson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package bbolt

import (
	"unsafe"
	"bytes"
)

/*
SECTION: public api.
Copyright (c) 2018 Simon Schmidt
*/

func radixPgid2bytes(p pgid) []byte {
	b := make([]byte,8)
	*((*pgid)(unsafe.Pointer(&b[0]))) = p
	return b
}
func radixBytes2Pgid(b []byte) (p pgid) {
	copy(((*[8]byte)(unsafe.Pointer(&p)))[:],b)
	return
}

/*
Implements a Radix tree in BoltDB, optimized for sparse nodes.
As a radix tree, it provides features, such as O(k) operations,
Minimum / Maximum value lookups and Ordered iteration.

Further reading:
- http://en.wikipedia.org/wiki/Radix_tree
- https://oscarforner.com/projects/tries
*/
type RadixBucket struct{
	acc radixAccess
}
func (r *RadixBucket) spill() error {
	return r.acc.persist()
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
func (r *RadixBucket) Get(key []byte) []byte {
	if len(key) == 0 {
		return nil
	} else if len(key) > MaxKeySize {
		return nil
	}
	return r.acc.get(key).leaf()
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value SHOULD remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction,
// if the key is blank, if the key is too large, or if the value is too large.
func (r *RadixBucket) Put(key,value []byte) error {
	if r.acc.tx.db==nil {
		return ErrTxClosed
	} else if !r.acc.tx.writable {
		return ErrTxNotWritable
	}
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value)==0 {
		return ErrValueRequired
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}
	if len(key)==0 { return ErrKeyRequired }
	
	r.acc.insert(key,value)
	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (r *RadixBucket) Delete(key []byte) error {
	if r.acc.tx.db==nil {
		return ErrTxClosed
	} else if !r.acc.tx.writable {
		return ErrTxNotWritable
	}
	if len(key) == 0 {
		return nil
	} else if len(key) > MaxKeySize {
		return nil
	}
	r.acc.del(key)
	return nil
}

// Iterator creates a iterator for this radix tree.
// The iterator is only valid as long as the transaction is open.
// The iterator initially points to the first key-value pair.
// For reverse iteration, please call Last().
// Do not use a iterator after the transaction is closed.
func (r *RadixBucket) Iterator() *RadixIterator {
	return &RadixIterator{r.acc.traversal()}
}

// Minimum performs a minimum key-value lookup.
func (r *RadixBucket) Minimum() (key,value []byte) {
	return r.acc.minPair()
}

// Maximum performs a maximum key-value lookup.
func (r *RadixBucket) Maximum() (key,value []byte) {
	return r.acc.maxPair()
}

type RadixIterator struct{
	trav *radixTraversal
}

// Reset resets the iterator to the first key-value pair in this radix tree.
func (r *RadixIterator) Reset() {
	r.trav.reset()
}
// Last resets the iterator to the last key-value pair in this radix tree.
func (r *RadixIterator) Last() {
	r.trav.last()
}

// Next obtains the next key-value pair from this radix tree.
func (r *RadixIterator) Next() (key,value []byte,ok bool) {
	return r.trav.next()
}

// Prev obtains the previous key-value pair from this radix tree.
//
// If Prev is called after Next, thus switching traversal order, glitches such
// as duplicate key-value pairs may occur.
func (r *RadixIterator) Prev() (key,value []byte,ok bool) {
	return r.trav.prev()
}

/*
SECTION: bindings.
Copyright (c) 2018 Simon Schmidt
Copyright (c) 2013-2018 coreos/etcd.io Authors
Copyright (c) 2013 Ben Johnson
*/
func (b *Bucket) obtainRadixBucket(k ,v []byte) *RadixBucket {
	if b.radixes==nil {
		return &RadixBucket{acc:radixAccess{tx:b.tx,root:radixBytes2Pgid(v)}}
	}
	if rad,ok := b.radixes[string(k)]; ok { return rad }
	if len(v)<8 { return nil }
	rad := &RadixBucket{acc:radixAccess{tx:b.tx,root:radixBytes2Pgid(v)}}
	b.radixes[string(k)] = rad
	return rad
}
func (b *Bucket) createOrObtainRadixBucket(key []byte,obtain bool) (*RadixBucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}
	
	// Move cursor to correct position.
	c := b.Cursor()
	k, v, flags := c.seek(key)
	
	// Return an error if there is an existing key.
	if bytes.Equal(key, k) {
		if (flags & radixLeafFlag) != 0 {
			if obtain { return b.obtainRadixBucket(k,v),nil }
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}
	
	p,err := b.tx.allocate(1)
	if err!=nil { return nil,err }
	(&radixNode{}).write(radixPageBuffer(p))
	p.flags = radixPageFlag
	
	key = cloneBytes(key)
	v = radixPgid2bytes(p.id)
	c.node().put(key, key, v, 0, radixLeafFlag)
	
	return b.obtainRadixBucket(key,v),nil
}

// CreateRadixBucket creates a new radix-tree bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateRadixBucket(key []byte) (*RadixBucket, error) {
	return b.createOrObtainRadixBucket(key,false)
}

// CreateRadixBucketIfExist creates a new radix-tree bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateRadixBucketIfExist(key []byte) (*RadixBucket, error) {
	return b.createOrObtainRadixBucket(key,true)
}

// RadixBucket retrieves a radix-tree bucket by name.
// Returns nil if the radix-tree bucket does not exist.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) RadixBucket(k []byte) *RadixBucket {
	c := b.Cursor()
	if b.radixes==nil {
		nk, v, flags := c.seek(k)
		if !bytes.Equal(k,nk) { return nil }
		if (flags & radixLeafFlag) == 0 { return nil }
		return &RadixBucket{acc:radixAccess{tx:b.tx,root:radixBytes2Pgid(v)}}
	}
	if rad,ok := b.radixes[string(k)]; ok { return rad }
	nk, v, flags := c.seek(k)
	if len(v)<8 { return nil }
	if !bytes.Equal(k,nk) { return nil }
	if (flags & radixLeafFlag) == 0 { return nil }
	rad := &RadixBucket{acc:radixAccess{tx:b.tx,root:radixBytes2Pgid(v)}}
	b.radixes[string(k)] = rad
	return rad
}

// CreateRadixBucket creates a new radix-tree bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateRadixBucket(key []byte) (*RadixBucket, error) { return tx.root.CreateRadixBucket(key) }

// CreateRadixBucketIfExist creates a new radix-tree bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateRadixBucketIfExist(key []byte) (*RadixBucket, error) { return tx.root.CreateRadixBucketIfExist(key) }

// RadixBucket retrieves a radix-tree bucket by name.
// Returns nil if the radix-tree bucket does not exist.
// The radix-tree bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) RadixBucket(key []byte) *RadixBucket { return tx.root.RadixBucket(key) }


