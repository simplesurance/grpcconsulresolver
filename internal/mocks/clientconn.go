package mocks

import (
	"errors"
	"sync"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type ClientConn struct {
	mutex                sync.Mutex
	addrs                []resolver.Address
	newAddressCallCnt    int
	lastReportedError    error
	updateStateReturnVal error
}

func NewClientConn() *ClientConn {
	return &ClientConn{}
}

func (t *ClientConn) ParseServiceConfig(_ string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{Err: errors.New("config parsing not implemented in test mock")}
}

func (t *ClientConn) ReportError(err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.lastReportedError = err
}

func (t *ClientConn) LastReportedError() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.lastReportedError
}

func (t *ClientConn) UpdateState(state resolver.State) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.addrs = state.Addresses
	t.newAddressCallCnt++

	return t.updateStateReturnVal
}

func (t *ClientConn) NewAddress(addrs []resolver.Address) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.addrs = addrs
	t.newAddressCallCnt++
}

func (t *ClientConn) UpdateStateCallCnt() int {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.newAddressCallCnt
}

func (t *ClientConn) Addrs() (addrs []resolver.Address) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.addrs
}

func (*ClientConn) NewServiceConfig(string) {}

func (t *ClientConn) SetUpdateStateReturnVal(v error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.updateStateReturnVal = v
}
