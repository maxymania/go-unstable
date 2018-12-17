# Persistent radix tree

Written: 17.12.2018

This document describes an on-disk, memory-mapped, Copy-on-Write implementation of a
[radix tree](http://en.wikipedia.org/wiki/Radix_tree), optimized for sparse nodes.

As a radix tree, it provides the following:

 - O(k) operations. In many cases, this can be faster than a hash table since
   the hash function is an O(k) operation, and hash tables have very poor cache locality.
 - Minimum / Maximum value lookups
 - Ordered iteration

Further reading: [Oscar Forner: Trie, TST and Radix Tree](https://oscarforner.com/projects/tries).

##### Optimized sparse Nodes

In both Tries and Radix Trees the child-node is obtained by an lookup to a datastructure associating a bytes
to nodes or node addresses. In most implementations, this datastructure is implemented as a simple array.
On sparse nodes, this would lead to a waste of storage space (memory or disk), as most array-slots are unused.

Instead of an array, we use a table sorted by key _(see Fig.1)_ implemented in a column oriented fashion _(see Fig.2)_.

See also: [The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases](https://db.in.tum.de/~leis/papers/ART.pdf) _(pdf)_

```
+------+--------+
| key  | value  |
+------+--------+
|   1  | 0x1200 |
|  15  | 0xbd18 |
|  42  | 0x9921 |
| 199  | 0xeaf9 |
| 249  | 0xf1a1 |
+------+--------+
Fig. 1: Example of a sorted key value table.

keys   := [...]byte   {1      ,15     ,42     ,199    ,249    }
values := [...]radixID{0x1200 ,0xbd18 ,0x9921 ,0xeaf9 ,0xf1a1 }
Fig. 2: The same table as in Fig.1, but implemented using arrays.
```

### On-Disk structure

Most Trie and Radix Tree implementations are implemented as pure volatile in-memory data structures rather than on-disk.
This implementation, however, is an on-disk radix-tree-implementation with Copy-on-Write semantics. It uses a page-based
allocation algorithm, comparable to those, commonly used for B-Trees.
In fact, the allocator, this radix tree is using, was designed for B-Trees.

#### The problem of fitting a nodes into pages optimally

A Page-based allocator is a perfect fit for B-Trees as the B-Tree structure is designed so that a Node within this Tree
corresponds to a page/block. However, other trees (such as Red-Black-Trees, AVL-trees, Tries and Radix Trees) are not
designed to make optimal use of pages/blocks.

We have to make a tradeof between

1. using one page for every node, thus wasting disk-space and causing poor cache-locality, or
2. stuffing multiple nodes into each page, and dealing with the complexity of references between pages.

Our solution is to implement a simplistic hybrid scheme with the following characteristics:

1. Each page has one root node. Each pointer to that page refers to the page's root node. _(see Fig. 3)_
2. Each node, other than the root node, is only referred from within the same page.
3. Each node-pointer refers either
   - to another page _(see rule 1)_
   - to another location within the same page _(see rule 2)_
4. The root-node is located at the first possible location within that page. _(see Fig. 4)_

```
      ...
       |
+------|------+
|      O      |
|     / \     |
|   O     O   |
+--/-\---/-\--+
  |   | |   |
 ... ..... ...
Fig. 3: A Page with one root-node and two 'inlined' child nodes.

+-------------------------------------------------------------------+
| [Page-Header][Root-Node......][Child-Node-1..][Child-Node-2.....] |
+-------------------------------------------------------------------+
Fig. 4: The typical page-layout of the page in Fig. 3
```

#### Pushing oversized values to seperate pages

Although the underlying allocator allows us to allocate larger contiguous areas, consisting of multiple pages, to store
oversized values in B-Trees (for which it was initially designed), it is highly undesireable to store large nodes,
as a node is rewritten if any of its descendants has changed, due to the CoW characteristics.
This would lead to Write amplification.

Usually a value are stored within the node, referred by the corresponding key, in a field called `leafIn`.
However, if a value is huge, then it is stored in the `leafIn` field of a seperate node, stored in a seperate page.
This seperate node is refered by the `leafEx` field of the node, that would otherwise contain the value.

### Updating the Tree

The Radix Tree is stored with CoW semantics. That means:

1. If a node is updated, it is written to a new location.
2. If a node is updated, all it's ascending nodes must be updated as well.

Also, pages are tightly packed, making in-place updates impossible.

In order to enable efficient updates on the Radix Tree, a node has two possible representations:

1. Stored in the SLS(single level storage)/on disk. Read-only.
2. Stored in heap. Writable.

Read operations can operate seemlessly on either the SLS representation or the heap representation.
A heap-node can refer to both SLS-nodes and heap-nodes. SLS-nodes can only refer to other SLS-nodes.

#### Deserializing nodes

In order to update a node, it must be converted from the SLS representation to the heap representation.
In order to deserialize a node, all ascending nodes must be deserialized before.
As a page can contain multiple nodes, we always deserialize the whole page.
As every deserialized page will be replaced during update, the page is added to the _freelist_ after deserialization.

#### Updating the tree

During insert/update and delete operations, every page that is traversed during this operation is deserialized and
added to the _freelist_, as it is assumed, that the traversed nodes will be rewritten.

#### Writing the heap-nodes to disk

The most important part of Updating is propably the step of writing the changed nodes to disk.
Without this step, the data would be lost after shutdown or power-off.

The central part of the write-back code is the `persist_walk()` function _(see Fig. 5)_.

```
def persist_walk(node):
  # do top-down work
  for child in node.egdes:
    # Skip all non-heap nodes.
    if child is SLS: continue
    
    # Traverse further
    persist_walk(child)
  # do bottom-up work

Fig. 5: Pseudocode illustrating the top-down and the bottom-up pass.
```

##### 'Inlining' child nodes

When written to disk, the nodes should be grouped into pages as possible. Otherwise, they implementation would
allocate a page for every node. Grouping takes place during the top-down phase of `persist_walk()`.
If the current node is considered the root of a page (eg. not marked as inlined), `persist_pack()`, which greedily
marks as many pages to be inlined, as possible without exceeding the available payload-size of the page. _(see Fig. 6)_

```
# psz is a pointer to an integer holding the remaining size of the page.
def persist_pack(node,psz):
  *psz -= node.size
  for child in node.egdes:
    # Skip all non-heap nodes.
    if child is SLS: continue
    
    # Skip nodes that won't fit in page.
    if *psz < child.size: continue
    
    mark child as inlined
    
    # Traverse further
    persist_pack(child,psz)

Fig. 6: Pseudocode illustrating the pack algorithm.
```

##### Storing the tree in nodes.

The step of storing nodes in pages in performed in the bottom-up phase of `persist_walk()`.
For every child that is not an SLS-node and not marked as inlined, `persist_walk()` calls `persist_writeHead()`, which allocates a new page, and then calls `persist_write()`. The function `persist_write()` recursively writes the node and all
inlined nodes into the page. _(see Fig. 7)_

```
# pnode : *heap-node
# prid : *node-id in SLS
def persist_writeHead(pnode,prid):
  pag,pageid = allocate page
  off = new(int)
  *off = 0
  
  # Persist! The node contained in pnode will be the root-node.
  persist_write(pag,off,pnode,prid)
  
  # overwrite the node-id with a reference to the page.
  *prid = pageID(pageid)

# pag : []byte
# off : *int
# pnode : *heap-node
# prid : *node-id in SLS
def persist_write(pag,off,pnode,prid):
  node = *pnode
  pos = *off
  *off += node.size
  
  for k,child in node.egdes:
    # Skip all non-heap children.
    if child is SLS: continue
    
    # Skip non-inlined children.
    unless child is inlined: continue
    
    # Traverse further
    persist_write(pag,off,&node.egde<HEAP>[k],&node.egde<SLS>[k])
  # end for
  
  node.write(pag[pos:])
  *prid = inlineID(pos)
  *pnode = nil
Fig. 7: Pseudocode illustrating the pack algorithm.
```
