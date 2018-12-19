# Radix Tree API

```go
// Create a tree
db.Update(func(tx *bbolt.Tx) error {
	r,_ := tx.CreateRadixBucketIfNotExists([]byte("myTree"))
	r.Put([]byte("foo"), []byte{1})
	r.Put([]byte("bar"), []byte{2})
	r.Put([]byte("foobar"), []byte{3})
    return nil
})

db.View(func(tx *bbolt.Tx) error {
	r,_ := tx.RadixBucket([]byte("myTree"))
	
	// Find the longest prefix match
	m, _ := r.GetLongestPrefix("foozip")
	if string(m) != "foo" {
    	panic("should be foo")
	}
})
```

### Known problems.

Radix Trees can be nested in Buckets. ~~Radix trees can't be deleted.~~
~~If a Bucket containing a radix tree is being deleted, this could lead to a resource leak or even to a crash.~~

~~Please create Radix trees only in the root bucket.~~

UPDATE: Radix trees can now be deleted.
