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

type radixSlice struct{
	slice *[]byte
	length int
}
func (r radixSlice) bytes() []byte { return (*r.slice)[:r.length] }
func (r radixSlice) appnd(o []byte) radixSlice{
	*r.slice = append((*r.slice)[:r.length],o...)
	r.length += len(o)
	return r
}

/*
This recursive function is rather a Proof-Of-Concept than a production-use tool.
*/
func radixTraverse(a radixAddr,prefix radixSlice,f func(key,value []byte) bool) bool {
	if buf := a.leaf() ; len(buf)!=0 {
		if !f(prefix.bytes(),buf) { return false }
	}
	for i,n := 0,a.n_edges(); i<n; i++ {
		edge := a.edge(i)
		if !radixTraverse(edge,prefix.appnd(edge.prefix()),f) { return false }
	}
	
	return true
}
func radixRTraverse(a radixAddr,prefix radixSlice,f func(key,value []byte) bool) bool {
	n := a.n_edges()
	i := n
	for i>0 {
		i--
		edge := a.edge(i)
		if !radixRTraverse(edge,prefix.appnd(edge.prefix()),f) { return false }
	}
	
	if buf := a.leaf() ; len(buf)!=0 {
		if !f(prefix.bytes(),buf) { return false }
	}
	return true
}
func (r *radixAccess) traverse(f func(key,value []byte) bool) {
	parent := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	radixTraverse(parent,radixSlice{slice:new([]byte)},f)
}

/*
A state-machine base variant of the recursive radixTraverse() function.
*/
type radixTraversalNode struct{
	parent *radixTraversalNode
	a      radixAddr
	prefix radixSlice
	i,n    int
}
func (r *radixTraversalNode) call() (nr *radixTraversalNode,key,value []byte) {
start:
	if r.i<0 {
		r.i=0
		if buf := r.a.leaf(); len(buf)!=0 { return r,r.prefix.bytes(),buf }
	}
	if r.i<r.n {
		edge := r.a.edge(r.i)
		r.i++
		r = &radixTraversalNode{r,edge,r.prefix.appnd(edge.prefix()),-1,edge.n_edges()}
		goto start
	}
	if r.parent!=nil {
		r = r.parent
		goto start
	}
	return
}
func (r *radixTraversalNode) callR() (nr *radixTraversalNode,key,value []byte) {
start:
	if r.i>0 {
		r.i--
		edge := r.a.edge(r.i)
		N := edge.n_edges()
		r = &radixTraversalNode{r,edge,r.prefix.appnd(edge.prefix()),N,N}
	}
	if r.i==0 {
		r.i--
		if buf := r.a.leaf(); len(buf)!=0 { key,value = r.prefix.bytes(),buf }
	}
	if r.i<0 {
		r = r.parent
	}
	nr = r
	if key!=nil && value!=nil { return }
	if nr==nil { return }
	goto start
}

func (r *radixTraversalNode) longestCommonPrefix(key []byte) (nr *radixTraversalNode,rest []byte) {
	for {
		if len(key)==0 { break }
		i,m,ok := r.a.lookup_i(key)
		if !ok { break }
		r.i = i+1
		r = &radixTraversalNode{r,m,r.prefix.appnd(m.prefix()),-1,m.n_edges()}
		key,ok = m.match(key)
		if !ok { break }
	}
	return r,key
}

type radixTraversal struct{
	slice radixSlice
	node *radixTraversalNode
	root radixAddr
}
func (r *radixTraversal) reset() {
	if r.slice.slice==nil { r.slice.slice = new([]byte) }
	r.node = &radixTraversalNode{nil,r.root,r.slice,-1,r.root.n_edges()}
}
func (r *radixTraversal) last() {
	if r.slice.slice==nil { r.slice.slice = new([]byte) }
	r.node = &radixTraversalNode{nil,r.root,r.slice,r.root.n_edges(),r.root.n_edges()}
}
func (r *radixTraversal) next() (key,value []byte,ok bool) {
	if r.node==nil { return }
	r.node,key,value = r.node.call()
	ok = r.node!=nil
	return
}
func (r *radixTraversal) prev() (key,value []byte,ok bool) {
	if r.node==nil { return }
	r.node,key,value = r.node.callR()
	ok = r.node!=nil
	return
}
func (r *radixTraversal) longestCommonPrefix(key []byte) (match,rest []byte) {
	r.reset()
	r.node,rest = r.node.longestCommonPrefix(key)
	match = key[:len(key)-len(rest)]
	return
}

func (r *radixAccess) traversal() *radixTraversal {
	parent := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	tv :=  &radixTraversal{root:parent}
	tv.reset()
	return tv
}

func (r *radixAccess) minPair() (key,value []byte) {
	prefix := radixSlice{slice:new([]byte)}
	a := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	for {
		if a.isNil() { return }
		if buf := a.leaf() ; len(buf)!=0 {
			return prefix.bytes(),buf
		}
		if a.n_edges()==0 { return }
		a = a.edge(0)
		prefix = prefix.appnd(a.prefix())
	}
	panic("unreachable")
}

func (r *radixAccess) maxPair() (key,value []byte) {
	prefix := radixSlice{slice:new([]byte)}
	a := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	for {
		if a.isNil() { return }
		if buf := a.leaf() ; len(buf)!=0 {
			key,value = prefix.bytes(),buf
		}
		ne := a.n_edges()
		if ne==0 { return }
		a = a.edge(ne-1)
		prefix = prefix.appnd(a.prefix())
	}
	panic("unreachable")
}
