//go:build mock

package buffer

import "unsafe"

// In mock mode, we keep references to prevent GC from collecting the backing arrays.
// This is necessary because we return unsafe.Pointer to the data.
var mockAllocations = make(map[unsafe.Pointer][]byte)

// allocBuffer allocates a buffer from the Go heap (mock mode).
func allocBuffer(size int) unsafe.Pointer {
	buf := make([]byte, size)
	ptr := unsafe.Pointer(&buf[0])
	mockAllocations[ptr] = buf
	return ptr
}

// freeBuffer is a no-op in mock mode; we just remove the reference.
func freeBuffer(ptr unsafe.Pointer, size int) {
	delete(mockAllocations, ptr)
}
