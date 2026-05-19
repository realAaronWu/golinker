//go:build !mock

package buffer

/*
#include <stdlib.h>
*/
import "C"
import "unsafe"

// allocBuffer allocates a buffer using C.malloc (real mode).
func allocBuffer(size int) unsafe.Pointer {
	return C.malloc(C.size_t(size))
}

// freeBuffer frees a buffer using C.free (real mode).
func freeBuffer(ptr unsafe.Pointer, size int) {
	C.free(ptr)
}
