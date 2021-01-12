package cutil

/*
#include <stdlib.h>

extern void release_lock(void*);
extern void stored_unlock(void*);

static inline void storeAndWait(void** c_ptr, void* go_ptr, void* v) {
	*c_ptr = go_ptr;
	stored_unlock(v);
	release_lock(v);
	*c_ptr = NULL;
	stored_unlock(v);
}
*/
import "C"

import (
	"sync"
	"unsafe"
)

// PtrGuard respresents a guarded Go pointer (pointing to memory allocated by Go
// runtime) stored in C memory (allocated by C)
type PtrGuard struct {
	stored, release sync.Mutex
	released        bool
}

// WARNING: using binary semaphores (mutexes) for signalling like this is quite
// a delicate task in order to avoid deadlocks or panics. Whenever changing the
// code logic, please review at least three times that there is no unexpected
// state possible. Usually the natural choice would be to use channels instead,
// but these can not easily passed to C code because of the pointer-to-pointer
// cgo rule, and would require the use of a Go object registry.

// NewPtrGuard writes the goPtr (pointing to Go memory) into C memory at the
// position cPtr, and returns a PtrGuard object.
func NewPtrGuard(cPtr *unsafe.Pointer, goPtr unsafe.Pointer) *PtrGuard {
	var v PtrGuard
	v.release.Lock()
	v.stored.Lock()
	go C.storeAndWait(cPtr, goPtr, unsafe.Pointer(&v))
	v.stored.Lock()
	return &v
}

// Release removes the guarded Go pointer from the C memory by overwriting it
// with NULL.
func (v *PtrGuard) Release() {
	if !v.released {
		v.released = true
		v.release.Unlock() // send release signal
		v.stored.Lock()    // wait for stored signal
	}
}

//export release_lock
func release_lock(p unsafe.Pointer) {
	v := (*PtrGuard)(p)
	v.release.Lock()
}

//export stored_unlock
func stored_unlock(p unsafe.Pointer) {
	v := (*PtrGuard)(p)
	v.stored.Unlock()
}

// for tests
func cMalloc(n uintptr) unsafe.Pointer {
	return C.malloc(C.size_t(n))
}

func cFree(p unsafe.Pointer) {
	C.free(p)
}
