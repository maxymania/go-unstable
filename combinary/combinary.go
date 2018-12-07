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

package combinary

import (
	"time"
	"io"
	"sort"
	"bytes"
	"github.com/vmihailenco/msgpack"
	"fmt"
)

type column struct{
	name  []byte     // Ascending order
	stamp time.Time  // Descending order
	value []byte     // Ascending order
}
func (c column) String() string { return fmt.Sprintf("%q@%s->%q",c.name,c.stamp.Format("20060102150405.000000000"),c.value) }
func (c column) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.EncodeMulti(c.name,c.stamp,c.value)
}
func (c *column) DecodeMsgpack(dec *msgpack.Decoder) error {
	return dec.DecodeMulti(&c.name,&c.stamp,&c.value)
}

type columnlist []column
func (cl columnlist) String() string {
	switch len(cl) {
	case 0: return "[]"
	case 1: return fmt.Sprintf("[%v]",cl[0])
	}
	var buf bytes.Buffer
	for _,c := range cl[1:] { fmt.Fprintf(&buf,",%v",c) }
	return fmt.Sprintf("[%v%v]",cl[0],&buf)
}
func (cl columnlist) Len() int { return len(cl) }
func (cl columnlist) Less(i,j int) bool {
	switch co := bytes.Compare(cl[i].name,cl[j].name); {
	case co<0: return true
	case co>0: return false
	}
	switch ti,tj := cl[i].stamp,cl[j].stamp; {
	case ti.After(tj): return true
	case tj.After(ti): return false
	}
	return bytes.Compare(cl[i].value,cl[j].value)<0
}
func (cl columnlist) Swap(i,j int) { cl[i],cl[j] = cl[j],cl[i] }
func (cl columnlist) more(k []byte) func(i int) bool { return func(i int) bool { return bytes.Compare(cl[i].name,k)>0 } }
func (cl columnlist) find(k []byte) (*column) {
	l := cl.Len()
	i := sort.Search(l,cl.more(k))
	if i==l { return nil }
	if !bytes.Equal(k,cl[i].name) { return nil }
	return &cl[i]
}

type record struct{
	delet time.Time
	cols  columnlist
}
func (r record) String() string { return fmt.Sprint("{",r.delet.Format("20060102150405.000000000"),"@",r.cols,"}") }
func (r record) EncodeMsgpack(enc *msgpack.Encoder) (err error) {
	err = enc.EncodeTime(r.delet) ; if err!=nil { return }
	for _,c := range r.cols {
		err = enc.Encode(c)
		if err!=nil { return }
	}
	return
}
func (r *record) DecodeMsgpack(dec *msgpack.Decoder) (err error) {
	var t time.Time
	var cp *column
	t,err = dec.DecodeTime() ; if err!=nil { return }
	if r.delet.Before(t) { r.delet = t }
	for {
		if len(r.cols)==cap(r.cols) {
			r.cols = append(r.cols,column{})[:len(r.cols)]
		}
		cp = &r.cols[:len(r.cols)+1][len(r.cols)]
		err = dec.Decode(cp)
		if err==io.EOF { return nil }
		if err!=nil { return }
		r.cols = r.cols[:len(r.cols)+1]
	}
	return
}
func (r *record) compact() {
	sort.Sort(r.cols)
	ncols := r.cols[:0]
	var name []byte
	
	for _,c := range r.cols {
		if !c.stamp.After(r.delet) { continue }
		switch {
		case len(ncols)==0,!bytes.Equal(name,c.name):
			ncols = append(ncols,c)
			name  = c.name
		}
	}
	r.cols = ncols
}

type Merger struct{
	rec record
	status bool
}
func (r *Merger) Add(b []byte) (err error) {
	err = msgpack.Unmarshal(b,&r.rec)
	r.status = false
	return
}
func (r *Merger) AddAll(bb [][]byte) (err error) {
	for _,b := range bb {
		err = msgpack.Unmarshal(b,&r.rec)
		if err!=nil { break }
	}
	r.rec.compact()
	r.status = true
	return
}
func (r *Merger) Compactify() {
	if !r.status {
		r.rec.compact()
		r.status = true
	}
}
func (r *Merger) Extract() []byte {
	if !r.status {
		r.rec.compact()
		r.status = true
	}
	b,_ := msgpack.Marshal(r.rec)
	return b
}

type kv struct{
	K,V []byte
}
type Row struct{
	kv []kv
}
func (r *Row) Put(k,v []byte) {
	i := sort.Search(len(r.kv),func(i int) bool { return bytes.Compare(r.kv[i].K,k)>=0 })
	if len(r.kv)>i {
		if bytes.Equal(r.kv[i].K,k) {
			r.kv[i].V = v
			return
		}
	}
	r.kv = append(r.kv,kv{k,v})
	sort.Slice(r.kv,func(i,j int) bool { return bytes.Compare(r.kv[i].K,r.kv[j].K)<0 })
}
func (r *Row) Upsert() []byte {
	var re record
	re.cols = make(columnlist,len(r.kv))
	t := time.Now().UTC()
	for i,e := range r.kv { re.cols[i] = column{e.K,t,e.V} }
	b,_ := msgpack.Marshal(re)
	return b
}

func DeleteRow() []byte {
	b,_ := msgpack.Marshal(time.Now().UTC())
	return b
}

type RowScanner struct{
	rec record
	status bool
}
func (r *RowScanner) Parse(raw []byte) error {
	if r.status { r.rec = record{} }
	r.status = true
	return msgpack.Unmarshal(raw,&r.rec)
}
func (r *RowScanner) String() string { return r.rec.String() }
func (r *RowScanner) Get(k []byte) (ts time.Time,v []byte) {
	col := r.rec.cols.find(k)
	if col!=nil { ts,v = col.stamp,col.value }
	return
}
func (r *RowScanner) Columns(f func(k []byte,ts time.Time,v []byte)) { for _,col := range r.rec.cols { f(col.name,col.stamp,col.value) } }

