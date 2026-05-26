//go:build !mock && numa

package buffer

/*
#cgo LDFLAGS: -lnuma
#include <numa.h>
#include <stdlib.h>

// numa_alloc_on_node_safe allocates size bytes on the specified NUMA node.
// Falls back to posix_memalign if NUMA is not available or allocation fails.
static void* numa_alloc_on_node_safe(size_t size, int node) {
    if (numa_available() < 0) {
        // NUMA not available, fall back to page-aligned alloc
        void* ptr = NULL;
        if (posix_memalign(&ptr, 4096, size) != 0) {
            return NULL;
        }
        return ptr;
    }
    void* ptr = numa_alloc_onnode(size, node);
    if (ptr == NULL) {
        // Fall back to page-aligned alloc
        if (posix_memalign(&ptr, 4096, size) != 0) {
            return NULL;
        }
    }
    return ptr;
}

// numa_free_safe frees memory allocated with numa_alloc_onnode.
static void numa_free_safe(void* ptr, size_t size) {
    if (numa_available() >= 0) {
        numa_free(ptr, size);
    } else {
        free(ptr);
    }
}
*/
import "C"
import "unsafe"

// numaNode is the NUMA node to allocate buffers on.
// Set via SetNUMANode before pool creation.
var numaNode int

// SetNUMANode configures the NUMA node for buffer allocation.
func SetNUMANode(node int) {
	numaNode = node
}

// allocBuffer allocates memory on the configured NUMA node using libnuma.
// Falls back to page-aligned allocation if NUMA is unavailable.
func allocBuffer(size int) unsafe.Pointer {
	return C.numa_alloc_on_node_safe(C.size_t(size), C.int(numaNode))
}

// freeBuffer frees memory allocated via NUMA-aware allocation.
func freeBuffer(ptr unsafe.Pointer, size int) {
	C.numa_free_safe(ptr, C.size_t(size))
}
