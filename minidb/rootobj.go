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
	"io"
	"bytes"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/table"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/util"
	
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type rootobj struct {
	o *opt.Options
	s storage.Storage
	pool *util.BufferPool
	cach *cache.Cache
	merger MergerMk
}
func (r *rootobj) createMutator(fn int64) (*mutator,error) {
	w,e := r.s.Create(storage.FileDesc{storage.TypeJournal,fn})
	if e!=nil { return nil,e }
	return &mutator{
		merger:r.merger,
		db:memdb.New(r.o.GetComparer(),1<<27),
		wr:journal.NewWriter(w),
		cl:w,
		fn:fn,
	},nil
}
func (r *rootobj) open(fn int64) (*reader,error) {
	fd := storage.FileDesc{storage.TypeTable,fn}
	f,e := r.s.Open(fd)
	if e!=nil { return nil,e }
	sz,e := f.Seek(0,io.SeekEnd)
	if e!=nil { f.Close(); return nil,e }
	tab,e := table.NewReader(f,sz,fd,&cache.NamespaceGetter{r.cach,uint64(fn)},r.pool,r.o)
	if e!=nil { f.Close(); return nil,e }
	return &reader{tab,fn},nil
}

/* ******************************************************************* */

func (r *rootobj) convert(fn int64) error {
	fd := storage.FileDesc{storage.TypeJournal,fn}
	ftmp := storage.FileDesc{storage.TypeTemp,fn}
	ftab := storage.FileDesc{storage.TypeTable,fn}
	f,e := r.s.Open(fd)
	if e!=nil { return e }
	defer f.Close()
	w,e := r.s.Create(ftmp)
	if e!=nil { return e }
	
	tw := table.NewWriter(w,r.o)
	
	mt := new(mutator)
	mt.merger = r.merger
	mt.db = memdb.New(r.o.GetComparer(),1<<27)
	e = mt.load(journal.NewReader(f, nil, false, true))
	if e!=nil {
		w.Close()
		r.s.Remove(ftmp)
		return e
	}
	iter := mt.NewIterator(nil)
	defer iter.Release()
	for i0 := iter.First(); i0 ; i0 = iter.Next() {
		tw.Append(iter.Key(),iter.Value())
	}
	e = tw.Close()
	if e!=nil {
		w.Close()
		r.s.Remove(ftmp)
		return e
	}
	e = w.Close()
	if e!=nil { r.s.Remove(ftmp); return e }
	e = r.s.Rename(ftmp,ftab)
	if e!=nil { r.s.Remove(ftmp); return e }
	
	return e
}
func (r *rootobj) transfer(seg Segment) (Segment,error) {
	ofd := seg.(i_numbered).hasFd()
	fn := ofd.Num
	// Don't transfer
	if ofd.Type==storage.TypeTable { return seg,nil }
	ftmp := storage.FileDesc{storage.TypeTemp,fn}
	ftab := storage.FileDesc{storage.TypeTable,fn}
	w,e := r.s.Create(ftmp)
	if e!=nil { return nil,e }
	
	tw := table.NewWriter(w,r.o)
	
	iter := seg.NewIterator(nil)
	for i0 := iter.First(); i0 ; i0 = iter.Next() {
		tw.Append(iter.Key(),iter.Value())
	}
	iter.Release()
	
	e = tw.Close()
	if e!=nil {
		w.Close()
		r.s.Remove(ftmp)
		return nil,e
	}
	e = w.Close()
	if e!=nil { r.s.Remove(ftmp); return nil,e }
	e = r.s.Rename(ftmp,ftab)
	if e!=nil { r.s.Remove(ftmp); return nil,e }
	
	xseg,e := r.open(fn)
	if e!=nil {
		r.s.Remove(ftab)
		return nil,e
	}
	
	seg.(i_close).closf()
	r.s.Remove(ofd)
	
	return  xseg,nil
}

/* ******************************************************************* */

func (r *rootobj) root(fn int64,iters []iterator.Iterator) (error) {
	ftmp := storage.FileDesc{storage.TypeTemp,fn}
	ftab := storage.FileDesc{storage.TypeTable,fn}
	var k_buf []byte
	var merger Merger
	
	w,e := r.s.Create(ftmp)
	if e!=nil { return e }
	
	tw := table.NewWriter(w,r.o)
	
	niter := iterator.NewMergedIterator(iters,r.o.GetComparer(),false)
	for i0 := niter.First(); i0 ; i0 = niter.Next() {
		key := niter.Key()
		if bytes.Equal(k_buf,key) {
			if merger == nil {
				merger = r.merger()
			}
			merger.Add(niter.Value())
			continue
		}
		k_buf = append(k_buf[:0],key...)
		if merger != nil {
			tw.Append(k_buf,merger.Extract())
		}
		merger = r.merger()
		merger.Add(niter.Value())
	}
	niter.Release()
	if merger != nil {
		tw.Append(k_buf,merger.Extract())
	}
	
	e = tw.Close()
	if e!=nil {
		w.Close()
		r.s.Remove(ftmp)
		return e
	}
	e = w.Close()
	if e!=nil { r.s.Remove(ftmp); return e }
	e = r.s.Rename(ftmp,ftab)
	if e!=nil { r.s.Remove(ftmp); return e }
	
	return nil
}

