/*
Copyright (c) 2021 Simon Schmidt

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


// A Search Package for unstable/bbolt.
package search

import (
	"github.com/maxymania/go-unstable/bbolt"
	"github.com/RoaringBitmap/roaring"
	"encoding/binary"
	"regexp"
	"bytes"
	"errors"
)

var E_Bucket_Full = errors.New("E_Bucket_Full")

var wchar = regexp.MustCompile(`\w+`)
var schar = regexp.MustCompile(`\S+`)

func mergeWords(text []byte) (targ []byte) {
	dup := make(map[string]bool)
	targ = make([]byte,0,len(text))
	for _,word := range wchar.FindAll(text,-1) {
		word = bytes.ToLower(word)
		if dup[string(word)] { continue }
		dup[string(word)] = true
		targ = append(targ,' ')
		targ = append(targ,word...)
	}
	return
}
func i2b(id uint32) []byte {
	key := make([]byte,4)
	binary.BigEndian.PutUint32(key,id)
	return key
}



type changer struct {
	bbolt.VisitorDefault
	add bool
	id uint32
	bmp *roaring.Bitmap
}
func (ch *changer) VisitBefore() {
	ch.bmp = roaring.New()
}
func (ch *changer) VisitEmpty(key []byte) bbolt.VisitOp {
	if ch.add == ch.bmp.Contains(ch.id) { return bbolt.VisitOpNOP() }
	if ch.add {
		ch.bmp.Add(ch.id)
	} else {
		ch.bmp.Remove(ch.id)
	}
	if ch.bmp.GetCardinality() == 0 { return bbolt.VisitOpDELETE() }
	b,_ := ch.bmp.ToBytes()
	return bbolt.VisitOpSET(b)
}
func (ch *changer) VisitFull(key, value []byte) bbolt.VisitOp {
	ch.bmp.FromBuffer(value)
	return ch.VisitEmpty(key)
}

type searcher struct {
	bbolt.VisitorDefault
	failed bool
	bmp *roaring.Bitmap
}
func (ch *searcher) VisitEmpty(key []byte) bbolt.VisitOp {
	ch.failed = true
	return bbolt.VisitOpNOP()
}
func (ch *searcher) VisitFull(key, value []byte) bbolt.VisitOp {
	d := roaring.New()
	d.FromBuffer(value)
	if ch.bmp!=nil {
		ch.bmp.And(d)
	} else {
		ch.bmp = d
	}
	if ch.bmp.GetCardinality() == 0 {
		ch.failed = true
	}
	return bbolt.VisitOpNOP()
}


var uso bbolt.UnsafeOp


type TextInserter struct {
	Doc, Index *bbolt.Bucket
}
func (ti *TextInserter) nextSequence() (id uint32, err error) {
	var id64 uint64
	id64 = ti.Doc.Sequence()
	if id64>0xFFFFFFFF { err = E_Bucket_Full; return }
	id = uint32(id64)
	_,err = ti.Doc.NextSequence()
	return
}

func (ti *TextInserter) insertWords(rec []byte) (id uint32, err error) {
	id, err = ti.nextSequence()
	
	if err!=nil { return }
	err = ti.Doc.Put(i2b(id),rec)
	if err!=nil { return }
	
	ch := &changer{add: true, id: id}
	for _,word := range schar.FindAll(rec,-1) {
		err = ti.Index.Accept(word,ch,true)
		if err!=nil { break }
	}
	return
}
func (ti *TextInserter) removeWords(id uint32) (err error) {
	key := i2b(id)
	rec := ti.Doc.Get(key)
	
	ch := &changer{add:false, id: id}
	for _,word := range schar.FindAll(rec,-1) {
		err = ti.Index.Accept(word,ch,true)
		if err!=nil { return }
	}
	ti.Doc.Delete(key)
	return
}

func (ti *TextInserter) InsertText(text []byte) (id uint32, err error) {
	rec := mergeWords(text)
	return ti.insertWords(rec)
}
func (ti *TextInserter) RemoveDoc(id uint32) (err error) {
	return ti.removeWords(id)
}
func (ti *TextInserter) Search(phrase []byte) (bmp *roaring.Bitmap, err error) {
	ch := new(searcher)
	for _,word := range wchar.FindAll(phrase,-1) {
		err = ti.Index.Accept(word,ch,false)
		if err!=nil { return }
		if ch.failed { break }
	}
	bmp = ch.bmp
	if bmp==nil || ch.failed { bmp = roaring.New() }
	return
}



