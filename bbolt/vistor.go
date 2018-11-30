/*
Copyright (c) 2018 Simon Schmidt

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

import "fmt"

/*
Interface to access a record.

This API is inspired by the internals of Kyoto Carbinet.
*/
type Visitor interface{
	// Preprocess the main operations.
	VisitBefore()
	// Postprocess the main operations.
	VisitAfter()
	// Visit a record.
	VisitFull(key,value []byte) VisitOp
	// Visit a empty record space.
	VisitEmpty(key []byte) VisitOp
	// Visit a bucket.
	VisitBucket(key []byte,bkt *Bucket)
}


type VisitorDefault struct{}

func (v VisitorDefault) VisitBefore() { }
func (v VisitorDefault) VisitAfter() { }
func (v VisitorDefault) VisitFull(key,value []byte) VisitOp { return VisitOp{} }
func (v VisitorDefault) VisitEmpty(key []byte) VisitOp { return VisitOp{} }
func (v VisitorDefault) VisitBucket(key []byte,bkt *Bucket) { }

const (
	voDELETE = 1<<iota
	voSET
	voCOPY // must copy value
	voNEWBUCKET // create a bucket
	voVISITBUCKET // visit the created bucket
)

/*
Visit operation, no-op, delete, or set.

This API is inspired by the internals of Kyoto Carbinet.
*/
type VisitOp struct{
	buf []byte
	flg uint8
}

// No operation. Could also be defined as VisitOp{}
func VisitOpNOP() VisitOp { return VisitOp{} }

// Remove the record.
func VisitOpDELETE() VisitOp { return VisitOp{nil,voDELETE} }

// Replace the record.
// Supplied buffer must remain valid for the life of the transaction.
func VisitOpSET(buf []byte) VisitOp { return VisitOp{buf,voSET} }

// Replace the record. And copy the buffer.
// Supplied buffer must remain valid until the DB calls another callback or the API returns.
func VisitOpSET_COPY(buf []byte) VisitOp { return VisitOp{buf,voSET|voCOPY} }

// Creates a new bucket.
func VisitOpNEW_BUCKET() VisitOp { return VisitOp{nil,voNEWBUCKET} }

// Creates a new bucket. After this command is executed, the .Accept method immediately calls
// the .VisitBucket() method with the newly created bucket.
func VisitOpNEW_BUCKET_VISIT() VisitOp { return VisitOp{nil,voNEWBUCKET|voVISITBUCKET} }


func (v VisitOp) isset(u uint8) bool {
	return (v.flg&u)==u
}
func (v VisitOp) del() bool { return (v.flg&voDELETE)==voDELETE }
func (v VisitOp) set() bool { return (v.flg&voSET)==voSET }
func (v VisitOp) bkt() bool { return (v.flg&voNEWBUCKET)==voNEWBUCKET }
func (v VisitOp) String() string {
	switch {
	case v.isset(voDELETE): return "DELETE"
	case v.isset(voSET):
		if v.isset(voCOPY) { return fmt.Sprintf("SET-COPY(%q)",v.buf) }
		return fmt.Sprintf("SET(%q)",v.buf)
	case v.isset(voNEWBUCKET): return "NEW_BUCKET"
	}
	return "NOP"
}

var visitOp_emptybuf = []byte{}
func (v VisitOp) getBuf() []byte {
	if v.isset(voCOPY) {
		if len(v.buf)==0 { return visitOp_emptybuf }
		return cloneBytes(v.buf)
	}
	return v.buf
}

