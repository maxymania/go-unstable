/*
Copyright (c) 2018 Simon Schmidt

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

func radixPgid2bytes(p pgid) []byte {
	b := make([]byte,8)
	*((*pgid)(unsafe.Pointer(&b[0]))) = p
	return b
}
func radixBytes2Pgid(b []byte) (p pgid) {
	copy(((*[8]byte)(unsafe.Pointer(&p)))[:],b)
	return
}

type RadixBucket struct{
	acc radixAccess
}
func (r *RadixBucket) spill() error {
	return r.acc.persist()
}
func (r *RadixBucket) Get(key []byte) []byte {
	if len(key) == 0 {
		return nil
	} else if len(key) > MaxKeySize {
		return nil
	}
	return r.acc.get(key).leaf()
}
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
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}
	if len(key)==0 { return ErrKeyRequired }
	
	r.acc.insert(key,value)
	return nil
}
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

/*
SECTION: bindings.
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
func (b *Bucket) CreateRadixBucket(key []byte) (*RadixBucket, error) {
	return b.createOrObtainRadixBucket(key,false)
}
func (b *Bucket) CreateRadixBucketIfExist(key []byte) (*RadixBucket, error) {
	return b.createOrObtainRadixBucket(key,true)
}
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
func (tx *Tx) CreateRadixBucket(key []byte) (*RadixBucket, error) { return tx.root.CreateRadixBucket(key) }
func (tx *Tx) CreateRadixBucketIfExist(key []byte) (*RadixBucket, error) { return tx.root.CreateRadixBucketIfExist(key) }
func (tx *Tx) RadixBucket(key []byte) *RadixBucket { return tx.root.RadixBucket(key) }


