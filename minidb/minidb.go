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
	"errors"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/cache"
	ldbe "github.com/syndtr/goleveldb/leveldb/errors"
)

var EAllocFailed = errors.New("EAllocFailed")

type PDB struct {
	Writ SegmentWriter
	Segs []Segment
	Merger MergerMk
}
func (p *PDB) Put(key, value []byte) error {
	return p.Writ.Put(key, value)
}
func (p *PDB) Get(key []byte) (value []byte,err error) {
	i := 0
	m := p.Merger()
	for _,seg := range p.Segs {
		v,e := seg.Get(key)
		if e!=nil { continue }
		m.Add(v)
		i++
	}
	if i==0 { return nil,ldbe.ErrNotFound }
	return m.Extract(),nil
}

type numalloc struct {
	m sync.Mutex
	ctr int64
	has map[int64]bool
}
func (n *numalloc) set(i int64) {
	n.m.Lock(); defer n.m.Unlock()
	if n.has==nil { n.has = make(map[int64]bool) }
	n.has[i] = true
}
func (n *numalloc) clear(i int64) {
	n.m.Lock(); defer n.m.Unlock()
	// LEMMA: if n.has is nil, then n.has[i] == false
	if !n.has[i] { return }
	delete(n.has,i)
}
func (n *numalloc) alloc(max int) (int64,bool) {
	n.m.Lock(); defer n.m.Unlock()
	if n.has==nil { n.has = make(map[int64]bool) }
	for ; max > 0 ; max-- {
		if n.has[n.ctr] {
			n.ctr++
			continue
		}
		n.has[n.ctr] = true
		return n.ctr,true
	}
	return 0,false
}


type DB struct {
	PDB
	rootobj
	na numalloc
	writ *segWriter
}
func OpenDB(
	s storage.Storage,
	o *opt.Options,
	pool *util.BufferPool,
	cach *cache.Cache,
	merger MergerMk,
) (*DB,error) {
	db := new(DB)
	db.s = s
	db.o = o
	db.pool = pool
	db.cach = cach
	db.merger = merger
	db.Merger = merger
	db.writ = new(segWriter)
	db.Writ = db.writ
	
	fds,e := s.List(storage.TypeJournal)
	if e!=nil { return nil,e }
	for _,fd := range fds {
		e = db.convert(fd.Num)
		if e!=nil { return nil,e }
	}
	
	fds,e = s.List(storage.TypeTable)
	if e!=nil { return nil,e }
	for _,fd := range fds {
		slf,e := db.open(fd.Num)
		if e!=nil { return nil,e }
		db.na.set(fd.Num)
		db.Segs = append(db.Segs,slf)
	}
	
	nid,ok := db.na.alloc(1024)
	if !ok { return nil,EAllocFailed }
	m,e := db.createMutator(nid)
	if e!=nil { return nil,e }
	db.Segs = append(db.Segs,m)
	db.writ.SegmentWriter = m
	
	return db,nil
}

