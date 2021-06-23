package statustracker

import (
	"sync"
)

type CircuitBreakerStatus int

const (
	defaultHalfOpenThreshold = 5
	defaultOpenThreshold     = 7

	Closed   = CircuitBreakerStatus(0)
	HalfOpen = CircuitBreakerStatus(1)
	Open     = CircuitBreakerStatus(2)
)

// StatusTracker tracks results for circuitbreakers and reports statuses
type StatusTracker struct {
	failures          uint
	results           []bool
	halfOpenThreshold uint
	openThreshold     uint
	status            CircuitBreakerStatus
	mux               sync.Mutex
}

// NewStatusTracker returns a new default status tracket
func NewStatusTracker() *StatusTracker {
	successes := make([]bool, 10)
	for i := range successes {
		successes[i] = true
	}

	return &StatusTracker{
		0,
		successes,
		defaultHalfOpenThreshold,
		defaultOpenThreshold,
		Closed,
		sync.Mutex{},
	}
}

// ReportResult takes a bool signifying the success of some action
// and updates the current CircuitBreakerStatus
func (t *StatusTracker) ReportResult(success bool) {
	t.mux.Lock()
	defer t.mux.Unlock()
	if !t.results[0] {
		t.failures--
	}
	if !success {
		t.failures++
	}
	t.results = append(t.results[1:], success)
	t.UpdateStatus()
}

// UpdateStatus updates the CircuitBreakerStatus based on success history
func (t *StatusTracker) UpdateStatus() {
	if t.failures < t.halfOpenThreshold {
		t.status = Closed
	} else if t.failures < t.openThreshold {
		t.status = HalfOpen
	} else {
		t.status = Open
	}
}

// Status gets the status
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
