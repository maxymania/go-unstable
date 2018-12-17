/*
Copyright (c) 2018 Simon Schmidt

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


// Extended implementation of a radix tree.
package eradix

import (
	"sort"
)
import "fmt"

type daddr uint

type node struct{
	dirty  bool
	leaf   []byte
	
	prefix []byte
	
	edges_k  []byte
	edges_vm []*node
	edges_vd []daddr
}
func (n *node) isLeaf() bool { return len(n.leaf)!=0 }
func (n *node) search(b byte) (m *node,d daddr,ok bool){
	l := len(n.edges_k)
	i := sort.Search(l, func(j int) bool {
		return n.edges_k[j] >= b
	})
	if i<l && n.edges_k[i]==b {
		m,d,ok = n.edges_vm[i],n.edges_vd[i],true
	}
	return
}
func (n *node) insert(b byte,m *node,d daddr) {
	l := len(n.edges_k)
	n.edges_k  = append(n.edges_k ,0)
	n.edges_vm = append(n.edges_vm,nil)
	n.edges_vd = append(n.edges_vd,0)
	i := sort.Search(l, func(j int) bool {
		return n.edges_k[j] >= b
	})
	
	if i<l {
		if n.edges_k[i]==b { panic("key already exists!") }
		copy(n.edges_k [i+1:],n.edges_k [i:])
		copy(n.edges_vm[i+1:],n.edges_vm[i:])
		copy(n.edges_vd[i+1:],n.edges_vd[i:])
	}
	n.edges_k[i] = b
	n.edges_vm[i] = m
	n.edges_vd[i] = d
}
func (n *node) del(b byte) (m *node,d daddr,ok bool){
	l := len(n.edges_k)
	i := sort.Search(l, func(j int) bool {
		return n.edges_k[j] >= b
	})
	if i<l && n.edges_k[i]==b {
		m,d,ok = n.edges_vm[i],n.edges_vd[i],true
		copy(n.edges_k [i:],n.edges_k [i+1:])
		copy(n.edges_vm[i:],n.edges_vm[i+1:])
		copy(n.edges_vd[i:],n.edges_vd[i+1:])
		l--
		n.edges_k  = n.edges_k [:l]
		n.edges_vm = n.edges_vm[:l]
		n.edges_vd = n.edges_vd[:l]
	}
	return
}

type cursor struct{
	namebuf []byte
}
func (c *cursor) nnode(key, value []byte) *node {
	return &node{
		dirty: true,
		leaf: value,
		prefix: key,
	}
}
func (c *cursor) setDirty(m *node) {
	m.dirty = true
}
func (c *cursor) splitNode(m *node,l int) {
	n := new(node)
	*n = *m
	n.prefix = n.prefix[l:]
	m.prefix = m.prefix[:l]
	m.leaf = nil
}
func (c *cursor) setLeaf(m *node,value []byte) {
	m.leaf = value
}
func (c *cursor) mergeChildNode(m *node) {
	n := m.edges_vm[0]
	// TODO: if n==nil { m.edges_vd[0]... }
	m.prefix = append(append([]byte{},m.prefix...),n.prefix...)
	m.leaf     = n.leaf
	m.edges_k  = n.edges_k
	m.edges_vm = n.edges_vm
	m.edges_vd = n.edges_vd
}
func (c *cursor) insert(parent *node,key,value []byte) {
	for {
		c.setDirty(parent)
		
		// Key exthausted
		if len(key)==0 {
			c.setLeaf(parent,value)
			return
		}
		
		m,_,ok := parent.search(key[0])
		if !ok {
			parent.insert(key[0],c.nnode(key,value),0)
			return
		}
		l := longestPrefix(m.prefix,key)
		if l<len(m.prefix) {
			c.setDirty(m)
			c.splitNode(m,l)
			if l<len(key) {
				m.insert(key[l],c.nnode(key[l:],value),0)
			} else {
				c.setLeaf(m,value)
			}
			return
		}
		key = key[l:]
		parent = m
	}
}
func (c *cursor) del(parent *node,key []byte) {
	for {
		c.setDirty(parent)
		
		// Key exthausted
		if len(key)==0 {
			panic("should not get here!")
		}
		
		m,_,ok := parent.search(key[0])
		if !ok {
			return
		}
		l := longestPrefix(m.prefix,key)
		if l==len(key) {
			c.setDirty(m)
			if l==len(m.prefix) {
				switch len(m.edges_k) {
				case 0:
					parent.del(key[0])
					if len(parent.leaf)==0 && len(parent.edges_k)==1 {
						c.mergeChildNode(parent)
					}
				case 1:
					c.setLeaf(m,nil)
					c.mergeChildNode(m)
				default:
					c.setLeaf(m,nil)
				}
				return
			}
		}
		key = key[l:]
		parent = m
	}
}
func (c *cursor) get(parent *node,key []byte) []byte {
	for {
		if len(key)==0 {
			return parent.leaf
		}
		m,_,ok := parent.search(key[0])
		if !ok {
			return nil
		}
		l := longestPrefix(m.prefix,key)
		if l<len(m.prefix) {
			return nil
		}
		key = key[l:]
		parent = m
	}
	panic("unreachable")
}
func (c *cursor) findLongestPrefix(parent *node,key []byte) (k,v []byte) {
	nb := c.namebuf[:0]
	for {
		if len(key)==0 {
			c.namebuf = nb
			return nb,parent.leaf
		}
		m,_,ok := parent.search(key[0])
		if !ok {
			c.namebuf = nb
			return nb,parent.leaf
		}
		l := longestPrefix(m.prefix,key)
		if l<len(m.prefix) {
			c.namebuf = nb
			return
		}
		key = key[l:]
		parent = m
		nb = append(nb,m.prefix...)
	}
	panic("unreachable")
}


func Test() {
	var c cursor
	root := new(node)
	
	c.insert(root,[]byte("foo"),[]byte("1"))
	c.insert(root,[]byte("bar"),[]byte("2"))
	c.insert(root,[]byte("foobar"),[]byte("3"))
	
	fmt.Println(root)
	fmt.Println("----")
	fmt.Printf("%q\n",c.get(root,[]byte("foo")))
	fmt.Printf("%q\n",c.get(root,[]byte("bar")))
	fmt.Printf("%q\n",c.get(root,[]byte("foobar")))
	fmt.Printf("%q\n",c.get(root,[]byte("fo")))
	
	c.del(root,[]byte("foo"))
	
	fmt.Println("----")
	fmt.Printf("%q\n",c.get(root,[]byte("foo")))
	fmt.Printf("%q\n",c.get(root,[]byte("bar")))
	fmt.Printf("%q\n",c.get(root,[]byte("foobar")))
	fmt.Printf("%q\n",c.get(root,[]byte("fo")))
	
	c.del(root,[]byte("foobar"))
	
	fmt.Println("----")
	fmt.Printf("%q\n",c.get(root,[]byte("foo")))
	fmt.Printf("%q\n",c.get(root,[]byte("bar")))
	fmt.Printf("%q\n",c.get(root,[]byte("foobar")))
	fmt.Printf("%q\n",c.get(root,[]byte("fo")))
	
	fmt.Println("OK")
}

