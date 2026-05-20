//go:build mock || !cgo

package rdma

import (
	"context"
	"sync/atomic"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// qpCounter is a package-level counter for QP number assignment.
var qpCounter uint32

// Compile-time interface checks.
var (
	_ api.CMAcceptor = (*MockCMAcceptor)(nil)
	_ api.CMDialer   = (*MockCMDialer)(nil)
)

// --- MockPD ---

// MockPD implements api.ProtectionDomain.
type MockPD struct{}

func (m *MockPD) Handle() unsafe.Pointer {
	return nil
}

// --- MockMR ---

// MockMR implements api.MemoryRegion.
type MockMR struct {
	addr   unsafe.Pointer
	length int
	lkey   uint32
	rkey   uint32
}

func (m *MockMR) Addr() unsafe.Pointer { return m.addr }
func (m *MockMR) Length() int           { return m.length }
func (m *MockMR) LKey() uint32          { return m.lkey }
func (m *MockMR) RKey() uint32          { return m.rkey }

// --- MockCQ ---

// MockCQ implements api.CompletionQueue.
type MockCQ struct {
	size int
}

func (m *MockCQ) Handle() unsafe.Pointer { return nil }
func (m *MockCQ) Size() int              { return m.size }

// --- MockQP ---

// MockQP implements api.QueuePair.
type MockQP struct {
	qpNum uint32
	state api.QueuePairState
}

func (m *MockQP) Handle() unsafe.Pointer { return nil }
func (m *MockQP) QPNum() uint32          { return m.qpNum }
func (m *MockQP) State() api.QueuePairState { return m.state }

func (m *MockQP) ModifyToInit() error {
	m.state = api.QPStateInit
	return nil
}

func (m *MockQP) ModifyToRTR(destQPN uint32, destLID uint16, destGID [16]byte) error {
	m.state = api.QPStateRTR
	return nil
}

func (m *MockQP) ModifyToRTS() error {
	m.state = api.QPStateRTS
	return nil
}

// --- MockVerbs ---

// MockVerbs implements api.Verbs with in-memory fakes.
type MockVerbs struct {
	deviceName  string
	postSendLog []api.SendWR
	postRecvLog []api.RecvWR
	closed      bool
}

// NewMockVerbs creates a new MockVerbs instance.
func NewMockVerbs() *MockVerbs {
	return &MockVerbs{}
}

func (m *MockVerbs) OpenDevice(devName string) error {
	m.deviceName = devName
	return nil
}

func (m *MockVerbs) AllocPD() (api.ProtectionDomain, error) {
	return &MockPD{}, nil
}

func (m *MockVerbs) CreateCQ(size int) (api.CompletionQueue, error) {
	return &MockCQ{size: size}, nil
}

func (m *MockVerbs) CreateQP(pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	num := atomic.AddUint32(&qpCounter, 1)
	return &MockQP{qpNum: num, state: api.QPStateReset}, nil
}

func (m *MockVerbs) RegMR(pd api.ProtectionDomain, addr unsafe.Pointer, length int, access api.AccessFlags) (api.MemoryRegion, error) {
	return &MockMR{
		addr:   addr,
		length: length,
		lkey:   1,
		rkey:   2,
	}, nil
}

func (m *MockVerbs) DeregMR(mr api.MemoryRegion) error {
	return nil
}

func (m *MockVerbs) PostSend(qp api.QueuePair, wr *api.SendWR) error {
	m.postSendLog = append(m.postSendLog, *wr)
	return nil
}

func (m *MockVerbs) PostRecv(qp api.QueuePair, wr *api.RecvWR) error {
	m.postRecvLog = append(m.postRecvLog, *wr)
	return nil
}

func (m *MockVerbs) Close() error {
	m.closed = true
	return nil
}

// GetPostSendLog returns the recorded send work requests.
func (m *MockVerbs) GetPostSendLog() []api.SendWR {
	return m.postSendLog
}

// GetPostRecvLog returns the recorded receive work requests.
func (m *MockVerbs) GetPostRecvLog() []api.RecvWR {
	return m.postRecvLog
}

// --- MockCMEventChannel ---

// MockCMEventChannel implements api.CMEventChannel with an injectable event channel.
type MockCMEventChannel struct {
	events chan *api.CMEvent
}

// NewMockCMEventChannel creates a new MockCMEventChannel with the given buffer size.
func NewMockCMEventChannel(bufSize int) *MockCMEventChannel {
	return &MockCMEventChannel{
		events: make(chan *api.CMEvent, bufSize),
	}
}

func (ch *MockCMEventChannel) Listen(ctx context.Context, addr string, port int) error {
	return nil
}

func (ch *MockCMEventChannel) GetEvent(ctx context.Context) (*api.CMEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case evt := <-ch.events:
		return evt, nil
	}
}

func (ch *MockCMEventChannel) AckEvent(event *api.CMEvent) error {
	return nil
}

func (ch *MockCMEventChannel) Close() error {
	close(ch.events)
	return nil
}

// InjectEvent sends an event into the channel for testing.
func (ch *MockCMEventChannel) InjectEvent(evt *api.CMEvent) {
	ch.events <- evt
}

// --- MockCMAcceptor ---

// MockCMAcceptor implements api.CMAcceptor with in-memory fakes.
type MockCMAcceptor struct{}

// NewMockCMAcceptor creates a new MockCMAcceptor.
func NewMockCMAcceptor() *MockCMAcceptor {
	return &MockCMAcceptor{}
}

func (a *MockCMAcceptor) AcceptConn(cmID unsafe.Pointer, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	num := atomic.AddUint32(&qpCounter, 1)
	return &MockQP{qpNum: num, state: api.QPStateRTS}, nil
}

// --- MockCMDialer ---

// MockCMDialer implements api.CMDialer with in-memory fakes.
type MockCMDialer struct{}

// NewMockCMDialer creates a new MockCMDialer.
func NewMockCMDialer() *MockCMDialer {
	return &MockCMDialer{}
}

func (d *MockCMDialer) Dial(ctx context.Context, addr string, port int, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, unsafe.Pointer, error) {
	num := atomic.AddUint32(&qpCounter, 1)
	return &MockQP{qpNum: num, state: api.QPStateRTS}, nil, nil
}

func (d *MockCMDialer) Disconnect(cmID unsafe.Pointer) error {
	return nil
}

func (d *MockCMDialer) Close() error {
	return nil
}
