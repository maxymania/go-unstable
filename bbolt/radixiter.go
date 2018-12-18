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
type radixTraversal struct{
	slice radixSlice
	node *radixTraversalNode
	root radixAddr
}
func (r *radixTraversal) reset() {
	if r.slice.slice==nil { r.slice.slice = new([]byte) }
	r.node = &radixTraversalNode{nil,r.root,r.slice,-1,r.root.n_edges()}
}
func (r *radixTraversal) next() (key,value []byte,ok bool) {
	if r.node==nil { return }
	r.node,key,value = r.node.call()
	ok = r.node!=nil
	return
}
func (r *radixAccess) traversal() *radixTraversal {
	parent := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	tv :=  &radixTraversal{root:parent}
	tv.reset()
	return tv
}

