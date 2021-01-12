package cutil

/*
#include <stdlib.h>

extern void guard_lock(void*);
extern void value_unlock(void*);

static inline void storeUntilRelease(void** c_ptr, void* go_ptr, void* v) {
	*c_ptr = go_ptr;
	value_unlock(v);
	guard_lock(v);
	*c_ptr = NULL;
	value_unlock(v);
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
	value, guard sync.Mutex
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
	// lock both locks so that any later Lock calls block
	v.guard.Lock()
	v.value.Lock()
	// start a background goroutine that lives until Release is called.
	// this calls a Cgo function, effectively pinning goPtr in place.
	// while the function runs cPtr will be filled with the value of
	// the goPtr and nulled out before it exits
	go C.storeUntilRelease(cPtr, goPtr, unsafe.Pointer(&v))
	v.value.Lock() // block until C function sets the pointer
	return &v
}

// Release removes the guarded Go pointer from the C memory by overwriting it
// with NULL.
func (v *PtrGuard) Release() {
	if !v.released {
		v.released = true
		v.guard.Unlock() // done guarding the pointer, unblock the goroutine
		v.value.Lock() // block until C function NULLs the pointer
	}
}

//export guard_lock
func guard_lock(p unsafe.Pointer) {
	v := (*PtrGuard)(p)
	v.guard.Lock()
}

//export value_unlock
func value_unlock(p unsafe.Pointer) {
	v := (*PtrGuard)(p)
	v.value.Unlock()
}

// for tests
func cMalloc(n uintptr) unsafe.Pointer {
	return C.malloc(C.size_t(n))
}

func cFree(p unsafe.Pointer) {
	C.free(p)
}
