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


package minidb

import (
	"bufio"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/vmihailenco/msgpack"
	"io"
)

type mutator struct {
	mutex sync.Mutex
	merger MergerMk
	db *memdb.DB
	wr *journal.Writer
	cl io.Closer
	fn int64
}
func (m *mutator) load(jr *journal.Reader) error {
	var key,value []byte
	multi := []interface{}{&key,&value}
	for {
		r,e := jr.Next()
		if e!=nil { return e }
		dec := msgpack.NewDecoder(r)
		n,e := dec.DecodeMapLen()
		if e!=nil { return e }
		for i:=0 ; i<n ; i++ {
			e = dec.DecodeMulti(multi...)
			if e!=nil { return e }
			m.iput(key,value)
		}
	}
	panic("unreachable")
}
func (m *mutator) Put(key, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	wr,err := m.wr.Next()
	if err!=nil { return err }
	bw := bufio.NewWriter(wr)
	enc := msgpack.NewEncoder(bw)
	enc.EncodeMapLen(1)
	enc.EncodeBytes(key)
	enc.EncodeBytes(value)
	bw.Flush()
	m.wr.Flush()
	return m.iput(key,value)
}
func (m *mutator) iput(key, value []byte) error {
	if v,e := m.db.Get(key); e==nil && len(v)!=0 {
		merg := m.merger()
		merg.Add(v)
		merg.Add(value)
		value = merg.Extract()
	}
	return m.db.Put(key,value)
}
func (m *mutator) Get(key []byte) ([]byte,error) {
	return m.db.Get(key)
}
func (m *mutator) NewIterator(slice *util.Range) iterator.Iterator {
	return m.db.NewIterator(slice)
}
func (m *mutator) hasFd() storage.FileDesc { return storage.FileDesc{storage.TypeJournal,m.fn} }
func (m *mutator) closf() {
	m.wr = nil
	m.cl.Close()
	m.cl = nil
}
var _ i_numbered = (*mutator)(nil)

