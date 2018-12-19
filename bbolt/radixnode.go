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


package bbolt

import (
	"unsafe"
)

const (
	radix_Inlined = iota
	radix_Block
)
const (
	radixf_mmap = 1<<iota
	radixf_inlined
)


func radixBinSearch(buf *[256]byte,n int,k byte) (int,bool) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j)>>1)
		if buf[h]<k {
			i = h + 1 // preserves buf[i-1]<k
		} else {
			j = h // preserves buf[j]>=k
		}
	}
	if i==n { return i,false }
	return i,buf[i]==k
}
func radixLongestPrefix(a,b []byte) int {
	if len(a)>len(b) { a,b = b,a }
	for i,e := range a {
		if b[i]!=e { return i }
	}
	return len(a)
}


type radixID uint64
func (r radixID) inlined() bool { return (r&7)==radix_Inlined }
func (r radixID) isPage() bool { return (r&7)==radix_Block }
func (r radixID) offset() uint64 { return uint64(r>>3) }

func radixInlineID(i int) radixID {
	i &= ^7
	return radixID(i)
}
func radixPageID(p pgid) radixID {
	return radixID(p<<3)|radix_Block
}

type radixNode struct{
	flags uint
	
	leafEx_p *radixNode
	leafEx_v radixID
	n_edges uint16
	
	edges_p [256]*radixNode
	edges_v [256]radixID
	edges_k [256]byte
	
	prefix []byte
	leafIn []byte
}
func (r *radixNode) insert(k byte) (i int,coll bool) {
	i,coll = radixBinSearch(&r.edges_k,int(r.n_edges),k)
	if coll { return }
	copy(r.edges_k[i+1:],r.edges_k[i:])
	copy(r.edges_v[i+1:],r.edges_v[i:])
	copy(r.edges_p[i+1:],r.edges_p[i:])
	r.edges_k[i] = k
	r.edges_v[i] = 0
	r.edges_p[i] = nil
	r.n_edges++
	return
}
func (r *radixNode) del(k byte) (i int,ok bool) {
	i,ok = radixBinSearch(&r.edges_k,int(r.n_edges),k)
	if !ok { return }
	copy(r.edges_k[i:],r.edges_k[i+1:])
	copy(r.edges_v[i:],r.edges_v[i+1:])
	copy(r.edges_p[i:],r.edges_p[i+1:])
	r.n_edges--
	r.edges_v[r.n_edges] = 0
	r.edges_p[r.n_edges] = nil
	return
}
func (r *radixNode) hasLeaf() bool {
	return len(r.leafIn)==0 && r.leafEx_v==0 && r.leafEx_p==nil
}
func (r *radixNode) transferLeaf(o *radixNode) {
	o.leafEx_p,o.leafEx_v,o.leafIn = r.leafEx_p,r.leafEx_v,r.leafIn
	r.leafEx_p,r.leafEx_v,r.leafIn = nil,0,nil
}
func (r *radixNode) edge_keys() []byte { return r.edges_k[:r.n_edges] }
func (r *radixNode) edge_collision() bool {
	b := make(map[byte]bool,r.n_edges)
	for _,e := range r.edges_k[:r.n_edges] {
		if b[e] { return true }
		b[e] = true
	}
	return false
}
func (r *radixNode) edge_nonsorted() bool {
	c := -1
	for _,e := range r.edges_k[:r.n_edges] {
		ee := int(e)
		if ee<c { return true }
		c = ee
	}
	return false
}

/*
{
	radixID leafEx      : 64;
	uint    len(prefix) : 23; //24
	uint    n_edges     :  9; // 8
	uint    len(leafIn) : 32;
	
	radixID edges_v[...]
	byte    edges_k[...]
	byte    prefix[...]
	byte    leafIn[...]
}
*/

func (r *radixNode) size() (i int) {
	i  = 16
	i += int(r.n_edges) * 9
	i += len(r.prefix)+len(r.leafIn)
	i += 7
	i &= ^7
	return
}
func (r *radixNode) size_without_leafIn() (i int) {
	i  = 16
	i += int(r.n_edges) * 9
	i += len(r.prefix)
	i += 7
	i &= ^7
	return
}

func (r *radixNode) write(buf []byte) {
	*(*radixID)(unsafe.Pointer(&buf[0])) = r.leafEx_v
	compound := (uint32(len(r.prefix))<<9) | uint32(r.n_edges)
	*(*uint32)(unsafe.Pointer(&buf[8])) = compound
	*(*uint32)(unsafe.Pointer(&buf[12])) = uint32(len(r.leafIn))
	
	ne := int(r.n_edges)
	copy(((*[256]radixID)(unsafe.Pointer(&buf[16])))[:ne],r.edges_v[:ne])
	copy(buf[16+(ne*8):],r.edges_k[:ne])
	copy(buf[16+(ne*9):],r.prefix)
	copy(buf[16+(ne*9)+len(r.prefix):],r.leafIn)
}
func (r *radixNode) read(buf []byte) {
	*r = radixNode{flags:radixf_mmap}
	r.leafEx_v = *(*radixID)(unsafe.Pointer(&buf[0]))
	compound := *(*uint32)(unsafe.Pointer(&buf[8]))
	r.n_edges = uint16(compound&0x1ff)
	pxl := int(compound>>9)
	
	lfil := int(*(*uint32)(unsafe.Pointer(&buf[12])))
	
	ne := int(r.n_edges)
	copy(r.edges_v[:ne],((*[256]radixID)(unsafe.Pointer(&buf[16])))[:ne])
	copy(r.edges_k[:ne],buf[16+(ne*8):])
	r.prefix = buf[16+(ne*9):][:pxl]
	r.leafIn = buf[16+(ne*9)+pxl:][:lfil]
}

func radixPageBuffer(p *page) []byte {
	return (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[:]
}

type radixAddr struct{
	b *page
	t *Tx
	p *radixNode
	v  radixID
}
func (a radixAddr) isNil() bool {
	return (a.p==nil) && (a.v==0)
}
func (a radixAddr) node() ([]byte,*page) {
	if a.v.inlined() {
		return (*[maxAllocSize]byte)(unsafe.Pointer(&a.b.ptr))[a.v.offset()<<3:],a.b
	}
	p := a.t.page(pgid(a.v.offset()))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[:],p
}

func (a radixAddr) decode() *radixNode {
	var b []byte
	if a.p==nil {
		b,_ = a.node()
	} else {
		b = make([]byte,a.p.size())
		a.p.write(b)
	}
	r := new(radixNode)
	r.read(b)
	return r
}
func (a radixAddr) leaf() []byte {
	if a.isNil() { return nil }
	if b := a.leafEx(); !b.isNil() {
		a = b
	}
	return a.leafIn()
}
func (a radixAddr) leafIn() (leaf []byte) {
	if a.p==nil {
		buf,_ := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[ 8])))
		len_leaf := *((*uint32)(unsafe.Pointer(&buf[12])))
		ne := compound & 0xff
		prefix := compound>>9
		leaf = buf[16+(ne*9)+prefix:][:len_leaf]
	} else {
		leaf = a.p.leafIn
	}
	return
}
func (a radixAddr) leafEx() (b radixAddr){
	if a.p==nil {
		buf,pag := a.node()
		b = a
		b.b = pag
		b.v = *(*radixID)(unsafe.Pointer(&buf[0]))
	} else {
		b = a
		b.p = a.p.leafEx_p
		b.v = a.p.leafEx_v
	}
	return
}
func (a radixAddr) n_edges() int{
	if a.p==nil {
		buf,_ := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[8])))
		return int(compound&0x1ff)
	} else {
		return int(a.p.n_edges)
	}
	panic("unreachable")
}
func (a radixAddr) edge(i int) (b radixAddr) {
	if a.p==nil {
		buf,pag := a.node()
		b = a
		b.b = pag
		b.v = (*[256]radixID)(unsafe.Pointer(&buf[16]))[i]
	} else {
		b = a
		b.p = a.p.edges_p[i]
		b.v = a.p.edges_v[i]
	}
	return
}
func (a radixAddr) edge_k(i int) (b byte) {
	if a.p==nil {
		buf,_ := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[8])))
		n_edges := int(compound&0x1ff)
		return (*[256]byte)(unsafe.Pointer(&buf[16+(n_edges*8)]))[i]
	} else {
		return a.p.edges_k[i]
	}
	return
}


func (a radixAddr) prefix() (prefix []byte) {
	if a.p==nil {
		buf,_ := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[8])))
		l_prefix := int(compound>>9)
		n_edges := int(compound&0x1ff)
		i := 16
		i += n_edges * 9
		prefix = buf[i:][:l_prefix]
	} else {
		prefix = a.p.prefix
	}
	return
}
func (a radixAddr) match(key []byte) (rest []byte,ok bool) {
	var prefix []byte
	if a.p==nil {
		buf,_ := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[8])))
		l_prefix := int(compound>>9)
		n_edges := int(compound&0x1ff)
		i := 16
		i += n_edges * 9
		prefix = buf[i:][:l_prefix]
	} else {
		prefix = a.p.prefix
	}
	i := radixLongestPrefix(prefix,key)
	ok = len(prefix)==i
	rest = key[i:]
	return
}
func (a radixAddr) lookup(key []byte) (b radixAddr,ok bool) {
	_,b,ok = a.lookup_i(key)
	return
}
func (a radixAddr) lookup_i(key []byte) (i int,b radixAddr,ok bool) {
	if a.p==nil {
		buf,pag := a.node()
		compound := *((*uint32)(unsafe.Pointer(&buf[8])))
		n_edges := int(compound&0x1ff)
		i,ok = radixBinSearch((*[256]byte)(unsafe.Pointer(&buf[16+(n_edges*8)])),n_edges,key[0])
		if !ok { return }
		b = a
		b.b = pag
		b.v = (*[256]radixID)(unsafe.Pointer(&buf[16]))[i]
	} else {
		i,ok = radixBinSearch(&a.p.edges_k,int(a.p.n_edges),key[0])
		if !ok { return }
		b = a
		b.p = a.p.edges_p[i]
		b.v = a.p.edges_v[i]
	}
	return
}

func radixDecodeSubtree(p *page,id radixID, root bool) *radixNode {
	if root {
		// Point at root node.
		id = 0
	} else {
		// We only decode inlined nodes.
		if !id.inlined() { return nil }
		// Null-IDs are treated as NIL.
		if id==0 { return nil }
	}
	n := new(radixNode)
	n.read(radixPageBuffer(p)[id.offset()<<3:])
	for i,l := 0,int(n.n_edges); i<l; i++ {
		n.edges_p[i] = radixDecodeSubtree(p,n.edges_v[i],false)
	}
	n.leafEx_p = radixDecodeSubtree(p,n.leafEx_v,false)
	return n
}


