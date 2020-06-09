// +build !luminous
//
// These values for seek only valid in mimic and later

package cephfs

/*
#cgo CPPFLAGS: -D_FILE_OFFSET_BITS=64
#define _GNU_SOURCE
#include <unistd.h>
*/
import "C"

const (
	// SeekHole is used with Seek to position the file to the next hole
	// (unallocated space).
	SeekHole = int(C.SEEK_HOLE)
	// SeekData is used with Seek to position the file to the next allocated
	// section of data.
	SeekData = int(C.SEEK_DATA)
)

func init() {
	seekValid[SeekHole] = true
	seekValid[SeekData] = true
}
