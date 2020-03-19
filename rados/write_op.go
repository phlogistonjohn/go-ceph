package rados

// #cgo LDFLAGS: -lrados
// #include <errno.h>
// #include <stdlib.h>
// #include <stdio.h>
// #include <rados/librados.h>
//
// void tinker(char * s) {
//     printf("s: %s\n \n", s);
//     s[0] = 'b';
//     printf("s: %s\n \n", s);
// }
// char * frink(char * s) {
//     return s;
// }
//
import "C"

import (
	//"fmt"
	"runtime"
	"unsafe"
)

// WriteOp manages a set of discrete actions that will be performed together
// atomically.
type WriteOp struct {
	op C.rados_write_op_t
}

// CreateWriteOp returns a newly constructed write operation.
func CreateWriteOp() *WriteOp {
	return &WriteOp{
		op: C.rados_create_write_op(),
	}
}

// Release the resources associated with this write operation.
func (w *WriteOp) Release() {
	C.rados_release_write_op(w.op)
}

// Operate will perform the operation(s).
func (w *WriteOp) Operate(ioctx *IOContext, oid string) error {
	cOid := C.CString(oid)
	defer C.free(unsafe.Pointer(cOid))

	runtime.GC()
	return getRadosError(int(C.rados_write_op_operate(
		w.op, ioctx.ioctx, cOid, nil, 0)))
}

// Create a rados object.
func (w *WriteOp) Create(exclusive CreateOption) {
	// category, the 3rd param, is deprecated and has no effect so we do not
	// implement it in go-ceph
	C.rados_write_op_create(w.op, C.int(exclusive), nil)
}

func (w *WriteOp) OmapSet(data map[string][]byte) {
	//!!!!!!!!!!!!!!
	/*
		var s C.size_t
		var c *C.char
		ptrSize := unsafe.Sizeof(c)

		c_keys := C.malloc(C.size_t(len(pairs)) * C.size_t(ptrSize))
		c_values := C.malloc(C.size_t(len(pairs)) * C.size_t(ptrSize))
		c_lengths := C.malloc(C.size_t(len(pairs)) * C.size_t(unsafe.Sizeof(s)))

		defer C.free(unsafe.Pointer(c_keys))
		defer C.free(unsafe.Pointer(c_values))
		defer C.free(unsafe.Pointer(c_lengths))

		i := 0
		for key, value := range pairs {
			// key
			c_key_ptr := (**C.char)(unsafe.Pointer(uintptr(c_keys) + uintptr(i)*ptrSize))
			*c_key_ptr = C.CString(key)
			defer C.free(unsafe.Pointer(*c_key_ptr))

			// value and its length
			c_value_ptr := (**C.char)(unsafe.Pointer(uintptr(c_values) + uintptr(i)*ptrSize))

			var c_length C.size_t
			if len(value) > 0 {
				*c_value_ptr = (*C.char)(unsafe.Pointer(&value[0]))
				c_length = C.size_t(len(value))
			} else {
				*c_value_ptr = nil
				c_length = C.size_t(0)
			}

			c_length_ptr := (*C.size_t)(unsafe.Pointer(uintptr(c_lengths) + uintptr(i)*ptrSize))
			*c_length_ptr = c_length

			i++
		}

		C.rados_write_op_omap_set(
			op,
			(**C.char)kref,
			(**C.char)(c_values),
			(*C.size_t)(c_lengths),
			C.size_t(len(pairs)))
	*/
	count := C.size_t(len(data))
	krefs := make([]*C.char, count)
	vrefs := make([]*C.char, count)
	vlens := make([]C.size_t, count)
	i := 0
	for key, value := range data {
		kstr := C.CString(key)
		defer C.free(unsafe.Pointer(kstr))
		krefs[i] = kstr

		if len(value) > 0 {
			bptr := C.CBytes(value)
			defer C.free(bptr)
			vrefs[i] = (*C.char)(bptr)
			vlens[i] = C.size_t(len(value))
		}
		i++
	}
	C.rados_write_op_omap_set(
		w.op,
		(**C.char)(unsafe.Pointer(&krefs[0])),
		(**C.char)(&vrefs[0]),
		(*C.size_t)(&vlens[0]),
		//		(**C.char)(unsafe.Pointer(&vrefs[0])),
		//		(*C.size_t)(unsafe.Pointer(&vlens[0])),
		count)
}

func (w *WriteOp) OmapRemoveKeys(keys []string) {
	count := C.size_t(len(keys))
	krefs := make([]*C.char, count)
	for i, key := range keys {
		krefs[i] = C.CString(key)
		defer C.free(unsafe.Pointer(krefs[i]))
	}
	C.rados_write_op_omap_rm_keys(
		w.op,
		(**C.char)(unsafe.Pointer(&krefs[0])),
		count)
}

func (w *WriteOp) OmapClear() {
	C.rados_write_op_omap_clear(w.op)
}
