package rados

// #cgo LDFLAGS: -lrados
// #include <errno.h>
// #include <stdlib.h>
// #include <rados/librados.h>
import "C"

// ObjectsIter assists in the iteration over objects in a pool.
type ObjectsIter struct {
	isOpen bool
	ctx    C.rados_list_ctx_t
}

// ObjectsIterEntry represents one result entry when fetching data
// from an ObjectsIter.
type ObjectsIterEntry struct {
	Entry     string
	Key       string
	Namespace string
}

// NewObjectsIter creates and opens an ObjectsIter.
func NewObjectsIter(ioctx *IOContext) (*ObjectsIter, error) {
	iter := &ObjectsIter{}
	if cerr := C.rados_nobjects_list_open(ioctx.ioctx, &iter.ctx); cerr < 0 {
		return nil, GetRadosError(int(cerr))
	}
	iter.isOpen = true
	return iter, nil
}

// Token returns a token marking the current position of the iterator.
// May be used in combination with Seek.
func (iter *ObjectsIter) Token() IterToken {
	return IterToken(C.rados_nobjects_list_get_pg_hash_position(iter.ctx))
}

// Seek repositions the iterator to a different hash position.
// May be used in combination with Token.
func (iter *ObjectsIter) Seek(token IterToken) {
	C.rados_nobjects_list_seek(iter.ctx, C.uint32_t(token))
}

// Next fetches the next object entry in the pool.
// When the iterator is exhausted error will be set to RadosErrorNotFound.
func (iter *ObjectsIter) Next() (*ObjectsIterEntry, error) {
	if !iter.isOpen {
		return nil, RadosErrorNotFound
	}
	var cEntry, cKey, cNamespace *C.char
	err := C.rados_nobjects_list_next(iter.ctx, &cEntry, &cKey, &cNamespace)
	if err != 0 {
		return nil, GetRadosError(int(err))
	}
	return &ObjectsIterEntry{
		Entry:     C.GoString(cEntry),
		Key:       C.GoString(cKey),
		Namespace: C.GoString(cNamespace),
	}, nil
}

// Close the iterator context.
// Iterators are not closed automatically at the end of iteration.
// It is safe to call Close multiple times on the same ObjectsIter.
func (iter *ObjectsIter) Close() {
	if !iter.isOpen {
		return
	}
	C.rados_nobjects_list_close(iter.ctx)
	iter.isOpen = false
}

// SendAll sends all objects and errors that can be read from the iterator to
// the channels passed to the function.
// When iteration is complete SendAll will close both channels.
func (iter *ObjectsIter) SendAll(results chan<- *ObjectsIterEntry, errs chan<- error) {
	for {
		entry, err := iter.Next()
		switch {
		case err == RadosErrorNotFound:
			errs <- nil
			close(results)
			close(errs)
			return
		case err != nil:
			errs <- err
			close(results)
			close(errs)
			return
		default:
			results <- entry
		}
	}
}

type Iter struct {
	i         *ObjectsIter
	err       error
	entry     string
	namespace string
}

type IterToken uint32

// Iter returns a Iterator object that can be used to list the object names in the current pool
func (ioctx *IOContext) Iter() (*Iter, error) {
	iter, err := NewObjectsIter(ioctx)
	if err != nil {
		return nil, err
	}
	return &Iter{i: iter}, nil
}

// Token returns a token marking the current position of the iterator. To be used in combination with Iter.Seek()
func (iter *Iter) Token() IterToken {
	return iter.i.Token()
}

func (iter *Iter) Seek(token IterToken) {
	iter.i.Seek(token)
}

// Next retrieves the next object name in the pool/namespace iterator.
// Upon a successful invocation (return value of true), the Value method should
// be used to obtain the name of the retrieved object name. When the iterator is
// exhausted, Next returns false. The Err method should used to verify whether the
// end of the iterator was reached, or the iterator received an error.
//
// Example:
//	iter := pool.Iter()
//	defer iter.Close()
//	for iter.Next() {
//		fmt.Printf("%v\n", iter.Value())
//	}
//	return iter.Err()
//
func (iter *Iter) Next() bool {
	entry, err := iter.i.Next()
	if err != nil {
		iter.err = err
		return false
	}
	iter.entry = entry.Entry
	iter.namespace = entry.Namespace
	return true
}

// Value returns the current value of the iterator (object name), after a successful call to Next.
func (iter *Iter) Value() string {
	if iter.err != nil {
		return ""
	}
	return iter.entry
}

// Namespace returns the namespace associated with the current value of the iterator (object name), after a successful call to Next.
func (iter *Iter) Namespace() string {
	if iter.err != nil {
		return ""
	}
	return iter.namespace
}

// Err checks whether the iterator has encountered an error.
func (iter *Iter) Err() error {
	if iter.err == RadosErrorNotFound {
		return nil
	}
	return iter.err
}

// Closes the iterator cursor on the server. Be aware that iterators are not closed automatically
// at the end of iteration.
func (iter *Iter) Close() {
	iter.i.Close()
}
