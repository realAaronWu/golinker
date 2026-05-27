//go:build mock

package cq

import "github.com/wua20/golinker/api"

// DefaultPollFunc returns a no-op PollFunc for mock builds.
// Tests inject their own PollFunc via PollerConfig.
func DefaultPollFunc() PollFunc {
	return func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		return nil, nil
	}
}
