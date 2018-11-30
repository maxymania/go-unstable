// +build !solaris

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

import mmapgo "github.com/edsrzf/mmap-go"

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) (err error) {
	if (db.writeref!=nil) && !hasflags(db.db_Flags,DB_SkipMsync) {
		err = (mmapgo.MMap)(db.writeref).Flush()
		if err!=nil { return }
	}

	/*
	This section will be executed if:
		1. The OS has no unified buffer cache (UBC), (OpenBSD)
		2. db.dataref is not used for writes.
	*/
	if osHasNoUBC && munmap_peekptr(db.dataref)!=munmap_peekptr(db.writeref) {
		// perform msync() the readonly mmap'ed area. Will flush the Read-Cache.
		err = (mmapgo.MMap)(db.dataref).Flush()
		if err!=nil { return }
	}

	if !hasflags(db.db_Flags,DB_SkipFsync) {
		err = db.file.Sync()
	}
	return
}
