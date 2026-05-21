// +build !mock

#ifndef GOLINKER_HOTPATH_H
#define GOLINKER_HOTPATH_H

#include <stdint.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

// repost_item_t: batch re-post of receive buffers
typedef struct {
    struct ibv_qp *qp;
    void *buf;
    uint32_t size;
    struct ibv_mr *mr;
} repost_item_t;

// send_item_t: batch post send
typedef struct {
    struct ibv_qp *qp;
    void *buf;
    uint32_t size;
    struct ibv_mr *mr;
    int flags;
    uint64_t wr_id;
} send_item_t;

// Accessor for ibv_wc.imm_data which lives in an anonymous union (inaccessible from CGo).
static inline uint32_t golinker_wc_imm_data(const struct ibv_wc *wc) {
    return wc->imm_data;
}

// Setter for ibv_send_wr.imm_data (also in an anonymous union).
static inline void golinker_wr_set_imm_data(struct ibv_send_wr *wr, uint32_t imm) {
    wr->imm_data = imm;
}

// Post a single receive work request. Returns 0 on success, errno on failure.
int golinker_post_recv_one(struct ibv_qp *qp, void *buf, uint32_t size,
                           struct ibv_mr *mr, uint64_t wr_id);

// Poll CQ for completions AND re-post previous batch receive buffers in one CGo call.
// Returns number of completions (>=0), or negative errno on failure.
int golinker_poll_and_repost(struct ibv_cq *cq, struct ibv_wc *wcs, int max_wcs,
                          repost_item_t *reposts, int repost_count);

// Post multiple sends chaining them via ibv_send_wr linked list for ONE ibv_post_send call.
// Returns 0 on success, errno on failure.
int golinker_batch_post_send(send_item_t *items, int count);

// Post a single send work request.
// Returns 0 on success, errno on failure.
int golinker_post_send_single(struct ibv_qp *qp, void *buf, uint32_t size,
                           struct ibv_mr *mr, uint64_t wr_id, int flags);

// Wait for a CM event with a timeout (milliseconds).
// Returns:
//   0  : event received (stored in *event)
//   1  : timeout (no event within timeout_ms)
//  -1  : error (poll or rdma_get_cm_event failed)
int golinker_get_cm_event_timeout(struct rdma_event_channel *ch,
                                  struct rdma_cm_event **event,
                                  int timeout_ms);

// Dump CM ID diagnostic info to stderr: device name, port, src/dst addresses.
void golinker_dump_cm_id(struct rdma_cm_id *id, const char *label);

#endif // GOLINKER_HOTPATH_H
