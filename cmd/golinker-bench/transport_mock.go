//go:build mock

package main

// Kept for build tag consistency. Connection setup is now handled by
// rdma.Listen() and rdma.Dial() in the internal/rdma package.
