// +build solaris

package bbolt


import (
	"os"
	"golang.org/x/sys/unix"
	"errors"
	"syscall"
)

type mMap []byte

func sMapRegion(f *os.File, length int, writable bool, offset int64) (m mMap, e error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}
	prot := syscall.PROT_READ
	if writable { prot |= syscall.PROT_WRITE }
	m,e = unix.Mmap(int(f.Fd()),offset,length,prot,syscall.MAP_SHARED)
	return
}

func (m mMap) Flush() error {
	return unix.Msync(m,syscall.MS_SYNC)
}
func (m *mMap) Unmap() error {
	err := unix.Munmap(*m)
	*m = nil
	return err
}

