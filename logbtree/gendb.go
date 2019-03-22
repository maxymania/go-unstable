/*
Copyright (c) 2019 Simon Schmidt

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


package logbtree
import "bytes"

func compare(a,b []byte) (i int) {
	i = bytes.Compare(a,b)
	
	/*
	return
	/*/
	switch i {
	case -1,0,1: return
	}
	
	panic("bytes.Compare(a,b) not in {-1,0,1}")
	// */
}

type iCursor interface{
	step() (k,v []byte)
}

type iTx interface{
	getCursor() iCursor
	getVal(key []byte) ([]byte,bool)
}

type iBucket interface{
	getCursor(tx iTx) iCursor
	getVal(tx iTx,key []byte) ([]byte,bool)
}

type tx0 struct{}
func (tx0) step() (k,v []byte) { return }
func (tx0) getCursor() iCursor { return tx0{} }
func (tx0) getVal(key []byte) ([]byte,bool) { return nil,true }

type iBucketVal struct{}
func (iBucketVal) getCursor(tx iTx) iCursor { return tx.getCursor() }
func (iBucketVal) getVal(tx iTx,key []byte) ([]byte,bool) { return tx.getVal(key) }

var iBucketTail iBucket = iBucketVal{}

type iCursorCE struct {
	head,tail iCursor
	
	started,needshift bool
	hk,tk,hv,tv []byte
}
func (i *iCursorCE) step() (k,v []byte) {
	if !i.started {
		i.hk,i.hv = i.head.step()
		i.tk,i.tv = i.tail.step()
		i.started = true
	}
	if i.needshift {
		if len(i.hk)==0 {
			i.tk,i.tv = i.tail.step()
		} else if len(i.tk)==0 {
			i.hk,i.hv = i.head.step()
		} else {
			switch compare(i.hk,i.tk) {
			case -1:// head < tail
				i.hk,i.hv = i.head.step()
			case 0: // head == tail
				i.hk,i.hv = i.head.step()
				i.tk,i.tv = i.tail.step()
			case 1: // head > tail
				i.tk,i.tv = i.tail.step()
			}
		}
		i.needshift = false
	}
	if len(i.hk)==0 { i.needshift = true; return i.tk,i.tv }
	if len(i.tk)==0 { i.needshift = true; return i.hk,i.hv }
	switch compare(i.hk,i.tk) {
	case -1,0: // head <= tail
		i.needshift = true
		return i.hk,i.hv
	case 1: // head > tail
		i.needshift = true
		return i.tk,i.tv
	}
	
	panic("unreachable")
}

var _ iBucket = (*iBucketCE)(nil)
type iBucketCE struct {
	head,tail iBucket
}

func (b *iBucketCE) getCursor(tx iTx) iCursor {
	hc := b.head.getCursor(tx)
	tc := b.tail.getCursor(tx)
	return &iCursorCE{ head:hc, tail:tc }
	//panic("n/i")
}
func (b *iBucketCE) getVal(tx iTx,key []byte) (v []byte,ok bool) {
	v,ok = b.head.getVal(tx,key)
	if !ok { v,ok = b.tail.getVal(tx,key) }
	return
}

