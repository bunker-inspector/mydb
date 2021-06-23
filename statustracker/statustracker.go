package statustracker

import (
	"sync"
)

type CircuitBreakerStatus int

const (
	Closed   = CircuitBreakerStatus(0)
	HalfOpen = CircuitBreakerStatus(1)
	Open     = CircuitBreakerStatus(2)
)

// statusTracker keeps tracks results for circuitbreakers and calculates statuses
type StatusTracker struct {
	failures          uint
	results           []bool
	halfOpenThreshold uint
	openThreshold     uint
	status            CircuitBreakerStatus
	mux               sync.Mutex
}

func NewStatusTracker() *StatusTracker {
	successes := make([]bool, 10)
	for i := range successes {
		successes[i] = true
	}

	return &StatusTracker{
		0,
		successes,
		5,
		7,
		Closed,
		sync.Mutex{},
	}
}

func (t *StatusTracker) ReportStatus(success bool) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if !t.results[0] {
		t.failures--
	}
	t.results = append(t.results[1:], success)
	t.UpdateStatus()
}

func (t *StatusTracker) UpdateStatus() {
	if t.failures < t.halfOpenThreshold {
		t.status = Closed
	} else if t.failures < t.openThreshold {
		t.status = HalfOpen
	} else {
		t.status = Open
	}
}

func (t *StatusTracker) Status() CircuitBreakerStatus {
	return t.status
}

func (t *StatusTracker) SetWindowSize(size int) {
	t.mux.Lock()
	defer t.mux.Unlock()

	// prune old results and update
	for len(t.results) > size {
		oldest := t.results[0]
		if !oldest {
			t.failures--
		}
		t.results = t.results[1:]
	}
	t.UpdateStatus()
}
