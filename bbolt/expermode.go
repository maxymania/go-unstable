/*
Copyright (c) 2018 Simon Schmidt
Copyright (c) 2018 coreos/etcd.io Authors
Copyright (c) 2013 Ben Johnson

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/


package bbolt

import "bytes"
import "context"

/*
Unsafe operations. These should only be used by users, who know, what they are
doing. Un-Careful use will corrupt the database.
*/
type UnsafeOp struct{}

/*
Visits, possibly inserts, a key-value pair into the given cursor. The cursor
must be positioned, as if cursor.Seek(key) was called previously. Rougely, it
is enough if the following precondition is met:

	cursor.previousKey < key <= cursor.currentKey

It if the cursor.Next() returns nil key and value and the previous key is lower
than the current key, the necessary precondition is met.

If an insert was performed, after this operation, the cursor should EIGHTER point
to the newly inserted key OR to the key following it (which was the current key
before that insertion). I observed, that the latter one is true, but this behavoir
is not guaranteed. Be careful!
*/
func (UnsafeOp) AcceptExcact(key []byte,c *Cursor,vis Visitor,writable bool) error {
	b := c.bucket
	if b.tx.db == nil {
		return ErrTxClosed
	} else if writable && !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	
	vis.VisitBefore()
	defer vis.VisitAfter()
	
	k, v, flags := c.keyValue()
	
	// We handle 3 cases:
	// Case 1: No such record!
	if !bytes.Equal(key,k) {
		vop := vis.VisitEmpty(key)
		switch {
		case vop.set():
			if !writable { return ErrInvalidWriteAttempt }
			key = cloneBytes(key)
			value := vop.getBuf()
			c.node().put(key, key, value, 0, 0)
		case vop.bkt():
			if !writable { return ErrInvalidWriteAttempt }
			var value = createInlineBucket()
			// Insert into node.
			key = cloneBytes(key)
			c.node().put(key, key, value, 0, bucketLeafFlag)
			if vop.isset(voVISITBUCKET) {
				vis.VisitBucket(k,b.Bucket(key))
			}
			return nil
		}
		return nil
	}
	
	// Case 2: Record is a Bucket.
	if (flags & bucketLeafFlag)!=0 {
		// Special case: visit a bucket.
		vis.VisitBucket(k,b.obtainBucket(k,v))
		return nil
	}
	if notValue(flags) { return nil }
	
	// Case 3: Record exists
	vop := vis.VisitFull(k,v)
	switch {
	case vop.set():
		if !writable { return ErrInvalidWriteAttempt }
		key = cloneBytes(key)
		value := vop.getBuf()
		c.node().put(key, key, value, 0, 0)
	case vop.del():
		if !writable { return ErrInvalidWriteAttempt }
		c.node().del(key)
	case vop.bkt():
		if !writable { return ErrInvalidWriteAttempt }
		// NOTE: We simply replace a key-value-pair with a bucket. So we don't have to delete it.
		// c.node().del(key)
		var value = createInlineBucket()
		// Insert into node.
		key = cloneBytes(key)
		c.node().put(key, key, value, 0, bucketLeafFlag)
		if vop.isset(voVISITBUCKET) {
			vis.VisitBucket(k,b.Bucket(key))
		}
		return nil
	}
	
	return nil
}

/*
LinearSeek seeks to a key by performing a linear search within the table. This only makes sense,
if the key you're searching for is near-by the current key, possibly even inside the same page.

If the cursor does not point to any key-value pair, it will start at the beginning.

Note: this function can be very slow, if a lot of key-value pairs have to be skipped. So be careful.
*/
func (UnsafeOp) LinearSeek(c *Cursor,ctx context.Context,seek []byte) (key []byte, value []byte) {
	var flags uint32
	key, value, flags = UnsafeOp{}.linearSeek(c,ctx,seek)
	if notValue(flags) {
		value = nil
	}
	return
}
func (UnsafeOp) linearSeek(c *Cursor,ctx context.Context,seek []byte) (key []byte, value []byte,flags uint32) {
	if len(c.stack)==0 { c.First() }
	key, value, flags = c.keyValue()
	switch bytes.Compare(seek,key) {
	case -1: goto reverse
	case 0: return
	case 1: goto forward
	}
reverse:
	// XXX: rather than having an internal .prev() method, the
	//      algorithm is implemented inside the public .Prev() method.
	key,value = c.Prev()
	for bytes.Compare(seek,key)<0 {
		if ctx.Err()!=nil { return nil,nil,0 }
		key,value = c.Prev()
	}
	return c.next()
forward:
	key, value, flags = c.next()
	for bytes.Compare(seek,key)>0 {
		if ctx.Err()!=nil { return nil,nil,0 }
		key, value, flags = c.next()
	}
	return
}

