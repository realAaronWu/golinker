## ADDED Requirements

### Requirement: Smart mode transitions from busy-poll to event-wait
In `PollModeSmart`, the poller SHALL busy-poll using `PollFunc` and track consecutive empty poll rounds. After `SpinCount` consecutive empty polls (default 1024), the poller SHALL arm CQ notification with `ReqNotifyCQ` and park on the completion channel FD.

#### Scenario: Sustained traffic keeps busy-poll active
- **WHEN** completions arrive on every poll cycle
- **THEN** the poller stays in busy-poll mode and never arms CQ notification

#### Scenario: Idle period triggers event-wait transition
- **WHEN** `SpinCount` consecutive polls return zero completions
- **THEN** the poller calls `ReqNotifyCQ`, performs one final drain, and parks on the comp channel FD

### Requirement: Smart mode transitions from event-wait to busy-poll on wake
When the poller wakes from event-wait (CQ completion channel signals), it SHALL acknowledge the CQ event, drain all pending completions via `PollFunc`, reset the empty-poll counter to zero, and transition back to busy-poll mode.

#### Scenario: Wake from event-wait resumes busy-poll
- **WHEN** the poller is parked in event-wait and a completion arrives
- **THEN** the goroutine wakes, drains completions, and resumes busy-polling with `idle_count = 0`

### Requirement: Smart mode handles CQs without completion channel
If a CQ has `CompChannelFD() == -1`, the poller in smart mode SHALL fall back to pure busy-poll for that CQ (never attempt to arm notification or park).

#### Scenario: Mixed CQ types in smart mode
- **WHEN** a poller in smart mode manages one CQ with a comp channel and one without
- **THEN** the poller busy-polls both CQs, and only parks on the comp-channel-enabled CQ's FD when idle

### Requirement: SpinCount is configurable
The `SpinCount` parameter SHALL be configurable via `PollerConfig` and respected by smart mode. The default value SHALL be 1024.

#### Scenario: Custom SpinCount
- **WHEN** `PollerConfig.SpinCount` is set to 5
- **THEN** the poller transitions to event-wait after 5 consecutive empty polls (not 1024)
