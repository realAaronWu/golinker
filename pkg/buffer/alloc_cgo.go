//go:build !mock && !numa

package buffer

/*
#include <stdlib.h>
#include <errno.h>

// posix_memalign_wrapper allocates page-aligned memory.
// Returns NULL on failure.
static void* page_aligned_alloc(size_t size) {
    void* ptr = NULL;
    int rc = posix_memalign(&ptr, 4096, size);
    if (rc != 0) {
        return NULL;
    }
    return ptr;
}
*/
import "C"
import "unsafe"

// SetNUMANode is a no-op in non-NUMA builds. Build with -tags numa to enable
// NUMA-aware allocation via libnuma.
func SetNUMANode(_ int) {}

// allocBuffer allocates page-aligned memory using posix_memalign.
// Page alignment is required for optimal RDMA NIC performance.
func allocBuffer(size int) unsafe.Pointer {
	ptr := C.page_aligned_alloc(C.size_t(size))
	if ptr == nil {
		return C.malloc(C.size_t(size))
	}
	return ptr
}

// freeBuffer frees a buffer using C.free (works for both malloc and posix_memalign).
func freeBuffer(ptr unsafe.Pointer, size int) {
	C.free(ptr)
}
