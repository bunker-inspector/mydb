package statustracker

import "sync"

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
	return &StatusTracker{
		0,
		make([]bool, 10),
		5,
		7,
		Closed,
		sync.Mutex{},
	}
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
