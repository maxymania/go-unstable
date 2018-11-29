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

import (
	"fmt"
	"unsafe"
	mmapgo "github.com/edsrzf/mmap-go"
)

type annotatedError struct{
	annot string
	err error
}
func (a annotatedError) Error() string {
	return fmt.Sprint(a.annot,": ",a.err)
}

// mmap memory maps a DB's data file.
func mmap(db *DB, sz int) error {
	datamap := mmapgo.RDONLY
	writemap := mmapgo.RDONLY
	switch{
	case hasflags(db.db_Flags,DB_WriteSharedMmap): datamap = mmapgo.RDWR
	case hasflags(db.db_Flags,DB_WriteSeperatedMmap): writemap = mmapgo.RDWR
	}

	if db.readOnly {
		datamap = mmapgo.RDONLY
		writemap = mmapgo.RDONLY
	} else if !hasflags(db.db_Flags,DB_DontTruncateOnMmap) {
		// Truncate the database to the size of the mmap.
		if err := db.file.Truncate(int64(sz)); err != nil {
			return annotatedError{"Truncate",err}
		}
	}

	// Map the data file to memory.
	b, err := mmapgo.MapRegion(db.file,sz,datamap,0,0)
	//b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return annotatedError{"MapRegion",err}
	}

	switch {
	case datamap==mmapgo.RDWR:
		db.writeref = b
	case writemap==mmapgo.RDWR:
		c, err := mmapgo.MapRegion(db.file,sz,writemap,0,0)
		if err != nil {
			return annotatedError{"MapRegion",err}
		}
		db.writeref = c
	}

	// XXX: assume MADV_RANDOM is the default mode.
	// madvise(b, MADV_RANDOM)

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

func munmap_peekptr(b []byte) *byte {
	if len(b)==0 { return nil }
	return &b[0]
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	if munmap_peekptr(db.dataref)==munmap_peekptr(db.writeref) {
		db.writeref = nil
	} else if db.writeref!=nil {
		err := (*mmapgo.MMap)(&db.writeref).Unmap()
		if err!=nil { err = annotatedError{"MMap.Unmap()",err} }
	}

	// Unmap using the original byte slice.
	db.data = nil
	err := (*mmapgo.MMap)(&db.dataref).Unmap()
	db.datasz = 0
	if err!=nil { err = annotatedError{"MMap.Unmap()",err} }
	return err
}

