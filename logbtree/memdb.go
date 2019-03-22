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

import "github.com/syndtr/goleveldb/leveldb/memdb"
import "github.com/syndtr/goleveldb/leveldb/comparer"
import "github.com/syndtr/goleveldb/leveldb/iterator"
import "github.com/vmihailenco/msgpack"
import "sync"
import "hash"
import "hash/fnv"
import "io"

func createMemDB(w io.Writer,capacity int) iBucket {
	log := new(logMemDB)
	log.db = memdb.New(comparer.DefaultComparer,capacity)
	log.writer = msgpack.NewEncoder(w)
	log.cs = fnv.New64a()
	return log
}

type logMemCursor struct {
	iter iterator.Iterator
	not1 bool
}

func (c *logMemCursor) step() (k,v []byte) {
	var ok bool
	if c.not1 {
		ok = c.iter.Next()
	} else {
		ok = c.iter.First()
		c.not1 = true
	}
	if ok {
		k = c.iter.Key()
		v = c.iter.Value()
	}
	return
}

type logMemDB struct {
	db *memdb.DB
	writer *msgpack.Encoder
	wl sync.Mutex
	cs hash.Hash64
}

func(db *logMemDB) getCursor(tx iTx) iCursor {
	return &logMemCursor{db.db.NewIterator(nil),false}
}
func(db *logMemDB) getVal(tx iTx,key []byte) ([]byte,bool) {
	val,err := db.db.Get(key)
	return val,err!=nil
}
func(db *logMemDB) put(key, value []byte) error {
	db.wl.Lock(); defer db.wl.Unlock()
	db.cs.Reset()
	db.cs.Write(key)
	db.cs.Write(value)
	err := db.writer.EncodeMulti(key,value,db.cs.Sum64())
	if err!=nil { db.db.Put(key,value) }
	return err
}

var _ iBucket = (*logMemDB)(nil)

