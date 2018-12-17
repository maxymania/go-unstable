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

// const radixPageFlag    = 0x20

type radixAccess struct{
	tx *Tx
	root  pgid
	head *radixNode
}
func (r *radixAccess) decodeRoot() {
	r.decodeChild(&r.root,&r.head)
}
func (r *radixAccess) decodeChild(id *pgid,hd **radixNode) {
	if (*hd)!=nil { return }
	p := r.tx.page(*id)
	if (p.flags&radixPageFlag)==0 { panic("trying to decode invalid page") }
	*hd = radixDecodeSubtree(p,0,true)
	r.tx.db.freelist.free(r.tx.meta.txid,p)
	*id = 0
}
func (r *radixAccess) decodeChild2(id radixID,hd **radixNode) {
	if (*hd)!=nil { return }
	if id.inlined() { panic("invalid state: decode inlined") }
	pid := pgid(id.offset())
	p := r.tx.page(pid)
	if (p.flags&radixPageFlag)==0 { panic("trying to decode invalid page") }
	*hd = radixDecodeSubtree(p,0,true)
	r.tx.db.freelist.free(r.tx.meta.txid,p)
}
func (r *radixAccess) mergeChildNode(parent *radixNode) {
	r.decodeChild2(parent.edges_v[0],&parent.edges_p[0])
	m  := parent.edges_p[0]
	nb := make([]byte,len(parent.prefix),len(parent.prefix)+len(m.prefix))
	copy(nb,parent.prefix)
	nb = append(nb,m.prefix...)
	m.prefix = nb
	*parent = *m
}

/*
SECTION: Radix Trie insertion, deletion and lookup
*/

func (r *radixAccess) insert(key,value []byte) {
	r.decodeRoot()
	parent := r.head
	for {
		if len(key)==0 {
			if parent.leafEx_v!=0 && !parent.leafEx_v.inlined() {
				r.tx.db.freelist.free(r.tx.meta.txid,r.tx.page(pgid(parent.leafEx_v.offset())))
			}
			parent.leafIn = cloneBytes(value)
		}
		i,ok := radixBinSearch(&parent.edges_k,int(parent.n_edges),key[0])
		if !ok {
			i,_ = parent.insert(key[0])
			parent.edges_p[i] = &radixNode{
				prefix: cloneBytes(key),
				leafIn: cloneBytes(value),
			}
			return
		}
		r.decodeChild2(parent.edges_v[i],&parent.edges_p[i])
		m := parent.edges_p[i]
		l := radixLongestPrefix(m.prefix,key)
		if l<len(m.prefix) {
			n := new(radixNode)
			*n = *m
			*m = radixNode{}
			m.prefix = n.prefix[:l]
			n.prefix = n.prefix[l:]
			m.n_edges = 0
			i,_ := m.insert(n.prefix[0])
			m.edges_p[i] = n
			if l<len(key) {
				o := &radixNode{
					prefix: cloneBytes(key[l:]),
					leafIn: cloneBytes(value),
				}
				i,_ := m.insert(key[l])
				m.edges_p[i] = o
			} else {
				m.leafIn = cloneBytes(value)
			}
			return
		}
		key = key[l:]
		parent = m
	}
}
func (r *radixAccess) del(key []byte) {
	r.decodeRoot()
	parent := r.head
	for {
		// Key exthausted
		if len(key)==0 {
			panic("should not get here!")
		}
		i,ok := radixBinSearch(&parent.edges_k,int(parent.n_edges),key[0])
		if !ok {
			// Nothing to delete.
			return
		}
		r.decodeChild2(parent.edges_v[i],&parent.edges_p[i])
		m := parent.edges_p[i]
		l := radixLongestPrefix(m.prefix,key)
		if l==len(key) {
			if l==len(m.prefix) {
				// key == m.prefix
				// That means, we found it.
				if m.leafEx_v!=0 && !m.leafEx_v.inlined() {
					r.tx.db.freelist.free(r.tx.meta.txid,r.tx.page(pgid(m.leafEx_v.offset())))
					m.leafEx_v = 0
				}
				m.leafIn = nil
				m.leafEx_p = nil
				switch m.n_edges {
				case 0:
					parent.del(key[0])
					if (!parent.hasLeaf()) && (len(parent.edges_k)==1) {
						r.mergeChildNode(parent)
					}
				case 1:
					r.mergeChildNode(m)
				}
				return
			}
		}
		key = key[l:]
		parent = m
	}
}

func (r *radixAccess) get(key []byte) (result radixAddr) {
	parent := radixAddr{t:r.tx,p:r.head,v:radixPageID(r.root)}
	for {
		if len(key)==0 {
			return parent
		}
		m,ok := parent.lookup(key)
		if !ok {
			return
		}
		key,ok = m.match(key)
		if !ok {
			return
		}
		parent = m
	}
	panic("unreachable")
}

/*
SECTION: Persisting the tree.
*/

func (r *radixAccess) persist() (err error) {
	if r.head==nil { return }
	err = r.persist_walk(r.head)
	if err!=nil { return }
	var rid radixID
	var pid pgid
	pid,err = r.persist_writeHead(&r.head,&rid)
	if err!=nil { return }
	r.root = pid
	return
}

func (r *radixAccess) persist_externalize_leaf(node *radixNode) int {
	pgsz := r.tx.db.pageSize
	off := (pgsz-1)+pageHeaderSize
	
	sz_1 := (off+node.size())/pgsz
	sz_2 := (off+node.size_without_leafIn())/pgsz
	if sz_2<sz_1 {
		node.leafEx_p = &radixNode{leafIn:node.leafIn}
		node.leafIn = nil
		return sz_2
	}
	return sz_1
}

// This function traverses all nodes that are in heap.
// It will not traverse nodes, that are in SLS.
func (r *radixAccess) persist_walk(node *radixNode) (err error) {
	
	// perform pack() on non-inlined nodes only.
	if (node.flags&radixf_inlined)==0 {
		count := r.persist_externalize_leaf(node)
		sz := (r.tx.db.pageSize*count)-pageHeaderSize
		r.persist_pack(node,&sz)
	}
	if node.leafEx_p!=nil {
		_,err = r.persist_writeHead(&node.leafEx_p,&node.leafEx_v)
		if err!=nil { return }
	}
	
	for i,n := 0,int(node.n_edges); i<n; i++ {
		// Skip non-heap children.
		if node.edges_p[i]==nil { continue }
		
		// Traverse further
		err = r.persist_walk(node.edges_p[i])
		if err!=nil { return }
		
		// Skip inlined children.
		if (node.edges_p[i].flags&radixf_inlined)!=0 { continue }
		
		_,err = r.persist_writeHead(&node.edges_p[i],&node.edges_v[i])
		if err!=nil { return }
	}
	return
}
func (r *radixAccess) persist_pack(node *radixNode,psz *int) {
	*psz -= node.size()
	for i,n := 0,int(node.n_edges); i<n; i++ {
		// Skip non-heap children.
		if node.edges_p[i]==nil { continue }
		
		// Skip too-large nodes.
		if *psz<node.edges_p[i].size() { continue }
		
		// Mark as inlined.
		node.edges_p[i].flags |= radixf_inlined
		
		// Traverse further
		r.persist_pack(node.edges_p[i],psz)
	}
}

/*
ALGO: Write a node + all inlined children into a page.
*/
func (r *radixAccess) persist_writeHead(pnode **radixNode,prid *radixID) (pgid,error) {
	pgsz := r.tx.db.pageSize
	size := (*pnode).size()+pageHeaderSize
	pag,err := r.tx.allocate((size+pgsz-1)/pgsz)
	if err!=nil { return 0,err }
	pag.flags = radixPageFlag
	r.persist_write(radixPageBuffer(pag),new(int),pnode,prid)
	*prid = radixPageID(pag.id)
	return pag.id,nil
}
func (r *radixAccess) persist_write(pag []byte,off *int,pnode **radixNode,prid *radixID) {
	node := *pnode
	
	pos := *off
	*off += node.size()
	
	for i,n := 0,int(node.n_edges); i<n; i++ {
		// Skip non-heap children.
		if node.edges_p[i]==nil { continue }
		
		// Skip non-inlined children.
		if (node.edges_p[i].flags&radixf_inlined)==0 { continue }
		
		// Traverse further
		r.persist_write(pag,off,&node.edges_p[i],&node.edges_v[i])
	}
	
	node.write(pag[pos:])
	*prid = radixInlineID(pos)
	*pnode = nil
}



