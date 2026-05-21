// +build !mock

#include "hotpath.h"

#include <errno.h>
#include <poll.h>
#include <string.h>

/* Maximum batch size for stack-allocated work request arrays. */
#define GOLINKER_MAX_BATCH 32

/*
 * golinker_repost_recv_single - helper to post a single receive buffer.
 * Returns 0 on success, errno on failure.
 */
static inline int golinker_repost_recv_single(struct ibv_qp *qp, void *buf,
                                           uint32_t size, struct ibv_mr *mr)
{
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = size;
    sge.lkey   = mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = (uintptr_t)buf;  /* use buffer address as wr_id for identification */
    wr.next    = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    int ret = ibv_post_recv(qp, &wr, &bad_wr);
    return ret;
}

/*
 * golinker_post_recv_one - Post a single receive work request.
 *
 * Returns:
 *   0     : success
 *   > 0   : errno from ibv_post_recv
 */
int golinker_post_recv_one(struct ibv_qp *qp, void *buf, uint32_t size,
                           struct ibv_mr *mr, uint64_t wr_id)
{
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = size;
    sge.lkey   = mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = wr_id;
    wr.next    = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(qp, &wr, &bad_wr);
}

/*
 * golinker_poll_and_repost - Combined poll + repost to minimize CGo crossings.
 *
 * Strategy:
 *   1. Re-post all receive buffers from the reposts array first. This ensures
 *      the receive queue stays full while we process the completions from the
 *      previous batch.
 *   2. Poll the completion queue for up to max_wcs completions.
 *
 * Returns:
 *   >= 0 : number of completions polled
 *   <  0 : negative errno on failure (first error encountered)
 */
int golinker_poll_and_repost(struct ibv_cq *cq, struct ibv_wc *wcs, int max_wcs,
                          repost_item_t *reposts, int repost_count)
{
    int i;
    int ret;

    /* Phase 1: Re-post receive buffers from previous batch. */
    if (reposts != NULL && repost_count > 0) {
        for (i = 0; i < repost_count; i++) {
            ret = golinker_repost_recv_single(
                reposts[i].qp,
                reposts[i].buf,
                reposts[i].size,
                reposts[i].mr
            );
            if (ret != 0) {
                /* Return negative errno; ret from ibv_post_recv is errno. */
                return -ret;
            }
        }
    }

    /* Phase 2: Poll the completion queue. */
    ret = ibv_poll_cq(cq, max_wcs, wcs);
    if (ret < 0) {
        return -errno;
    }

    return ret;
}

/*
 * golinker_batch_post_send - Post multiple send work requests in a single
 * ibv_post_send call by chaining them via the linked list.
 *
 * All items MUST target the same QP (items[0].qp is used).
 * count must be <= GOLINKER_MAX_BATCH; excess items are silently clamped.
 *
 * Returns:
 *   0     : success
 *   > 0   : errno from ibv_post_send
 */
int golinker_batch_post_send(send_item_t *items, int count)
{
    struct ibv_send_wr wr[GOLINKER_MAX_BATCH];
    struct ibv_sge     sge[GOLINKER_MAX_BATCH];
    struct ibv_send_wr *bad_wr = NULL;
    int i;
    int ret;

    if (items == NULL || count <= 0) {
        return EINVAL;
    }

    /* Clamp to maximum batch size to avoid stack overflow. */
    if (count > GOLINKER_MAX_BATCH) {
        count = GOLINKER_MAX_BATCH;
    }

    memset(wr, 0, sizeof(struct ibv_send_wr) * count);
    memset(sge, 0, sizeof(struct ibv_sge) * count);

    for (i = 0; i < count; i++) {
        /* Set up scatter-gather entry. */
        sge[i].addr   = (uintptr_t)items[i].buf;
        sge[i].length = items[i].size;
        sge[i].lkey   = items[i].mr->lkey;

        /* Set up work request. */
        wr[i].wr_id      = items[i].wr_id;
        wr[i].sg_list    = &sge[i];
        wr[i].num_sge    = 1;
        wr[i].opcode     = IBV_WR_SEND;
        wr[i].send_flags = items[i].flags;

        /* Chain to next work request. */
        if (i < count - 1) {
            wr[i].next = &wr[i + 1];
        } else {
            wr[i].next = NULL;
        }
    }

    /* Single ibv_post_send call for the entire batch. */
    ret = ibv_post_send(items[0].qp, &wr[0], &bad_wr);
    return ret;
}

/*
 * golinker_post_send_single - Post a single send work request.
 *
 * This is the simplest path for latency-sensitive single-message sends.
 *
 * Returns:
 *   0     : success
 *   > 0   : errno from ibv_post_send
 */
int golinker_post_send_single(struct ibv_qp *qp, void *buf, uint32_t size,
                           struct ibv_mr *mr, uint64_t wr_id, int flags)
{
    struct ibv_sge sge;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr = NULL;
    int ret;

    if (qp == NULL || buf == NULL || mr == NULL) {
        return EINVAL;
    }

    memset(&sge, 0, sizeof(sge));
    sge.addr   = (uintptr_t)buf;
    sge.length = size;
    sge.lkey   = mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = wr_id;
    wr.next       = NULL;
    wr.sg_list    = &sge;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_SEND;
    wr.send_flags = flags;

    ret = ibv_post_send(qp, &wr, &bad_wr);
    return ret;
}

/*
 * golinker_get_cm_event_timeout - Wait for a CM event with a timeout.
 *
 * Uses poll() on the event channel FD so the caller can implement
 * context-aware cancellation by calling with short timeouts in a loop.
 *
 * Returns:
 *   0  : event received (*event is set)
 *   1  : timeout (no event within timeout_ms)
 *  -1  : error (errno is set)
 */
int golinker_get_cm_event_timeout(struct rdma_event_channel *ch,
                                  struct rdma_cm_event **event,
                                  int timeout_ms)
{
    struct pollfd pfd;
    int ret;

    pfd.fd = ch->fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    ret = poll(&pfd, 1, timeout_ms);
    if (ret == 0) {
        return 1;   /* timeout */
    }
    if (ret < 0) {
        return -1;  /* poll error */
    }

    /* FD is readable; rdma_get_cm_event should return immediately. */
    ret = rdma_get_cm_event(ch, event);
    if (ret != 0) {
        return -1;
    }

    return 0;
}
