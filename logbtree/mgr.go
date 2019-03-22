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

import (
	"fmt"
	"path/filepath"
	"github.com/maxymania/go-unstable/bbolt"
	"sync/atomic"
	"time"
	"os"
)


type database struct{
	path string
	db *bbolt.DB
	bktkey []byte
	//bktchn iBucket
	bktchn *iBucketCE
	memdb *logMemDB
	fetch,file *uint64
	logfile string
}
func (db *database) nextfilename() string {
	return filepath.Join(db.path,fmt.Sprintf("wal.%x.log",atomic.AddUint64(db.file,1)))
}
func (db *database) Get(key []byte) (val []byte,err error) {
	var tx *bbolt.Tx
	if tx,err = db.db.Begin(false); err!=nil { return }
	defer tx.Rollback()
	tx2 := makeDDB(tx.Bucket(db.bktkey))
	val,_ = db.bktchn.getVal(tx2,key)
	return
}
func (db *database) Put(key,value []byte) error {
	err := db.memdb.put(key,value)
	if err==nil { atomic.AddUint64(db.fetch,uint64(len(key)+len(value))) }
	return err
}
func (db *database) commitloop(d time.Duration,mz uint64,cp int) {
	tick := time.Tick(d)
	for {
		if atomic.LoadUint64(db.fetch) < mz {
			<- tick
		} else {
			db.commit(cp)
		}
	}
}
func (db *database) commit(cp int) {
	nlogfile := db.nextfilename()
	f,e := os.Create(nlogfile)
	if e!=nil { return }
	logfile := db.logfile
	memdb := db.memdb
	bktchn := db.bktchn
	
	e = db.db.Update(func(tx *bbolt.Tx) error {
		bkt,err := tx.CreateBucketIfNotExists(db.bktkey)
		if err!=nil { return err }
		iter := memdb.db.NewIterator(nil)
		for ok := iter.First(); ok ; ok = iter.Next() {
			bkt.Put(iter.Key(),iter.Value())
		}
		return nil
	})
	
	if e!=nil {
		f.Close()
		os.Remove(nlogfile)
		return
	}
	
	nmdb := createMemDB(f,cp)
	db.bktchn,db.memdb = &iBucketCE{ head: nmdb , tail: bktchn }  ,  nmdb.(*logMemDB)
	os.Remove(logfile)
}


