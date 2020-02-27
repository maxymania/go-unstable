// +build solaris

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
	datamap := false
	writemap := false
	switch{
	case hasflags(db.db_Flags,DB_WriteSharedMmap): datamap = true
	case hasflags(db.db_Flags,DB_WriteSeperatedMmap): writemap = true
	}

	if db.readOnly {
		datamap = false
		writemap = false
	} else if !hasflags(db.db_Flags,DB_DontTruncateOnMmap) {
		// Truncate the database to the size of the mmap.
		if err := db.file.Truncate(int64(sz)); err != nil {
			return annotatedError{"Truncate",err}
		}
	}

	// Map the data file to memory.
	b, err := sMapRegion(db.file,sz,datamap,0)
	//b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return annotatedError{"MapRegion",err}
	}

	switch {
	case datamap:
		db.writeref = b
	case writemap:
		c, err := sMapRegion(db.file,sz,writemap,0)
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
		err := (*mMap)(&db.writeref).Unmap()
		if err!=nil { err = annotatedError{"MMap.Unmap()",err} }
	}

	// Unmap using the original byte slice.
	db.data = nil
	err := (*mMap)(&db.dataref).Unmap()
	db.datasz = 0
	if err!=nil { err = annotatedError{"MMap.Unmap()",err} }
	return err
}


// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) (err error) {
	if (db.writeref!=nil) && !hasflags(db.db_Flags,DB_SkipMsync) {
		err = (mMap)(db.writeref).Flush()
		if err!=nil { return }
	}

	/*
	This section will be executed if:
		1. The OS has no unified buffer cache (UBC), (OpenBSD)
		2. db.dataref is not used for writes.
	
	if osHasNoUBC && munmap_peekptr(db.dataref)!=munmap_peekptr(db.writeref) {
		// perform msync() the readonly mmap'ed area. Will flush the Read-Cache.
		err = (mMap)(db.dataref).Flush()
		if err!=nil { return }
	}
	*/

	if !hasflags(db.db_Flags,DB_SkipFsync) {
		err = db.file.Sync()
	}
	return
}

