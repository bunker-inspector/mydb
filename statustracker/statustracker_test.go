package statustracker

import "testing"

func TestStatusTransitions(t *testing.T) {
	tracker := NewStatusTracker()

	status := tracker.Status()
	if tracker.Status() != Closed {
		t.Errorf("Expected 'Closed' (%d) status. Got %d\n", Closed, status)
		t.FailNow()
	}
	for i := 0; i < defaultHalfOpenThreshold; i++ {
		tracker.ReportResult(false)
	}
	status = tracker.Status()
	if tracker.Status() != HalfOpen {
		t.Errorf("Expected 'HalfOpen' (%d) status. Got %d\n", HalfOpen, status)
		t.FailNow()
	}
	for i := 0; i < defaultOpenThreshold-defaultHalfOpenThreshold; i++ {
		tracker.ReportResult(false)
	}
	status = tracker.Status()
	if tracker.Status() != Open {
		t.Errorf("Expected 'Open' (%d) status. Got %d\n", Open, status)
		t.FailNow()
	}
}

func TestTrackerBoundaries(t *testing.T) {
	tracker := NewStatusTracker()

	tracker.ReportResult(true)
	if tracker.failures != 0 {
		t.Error("It should not be possible for failures to go below 0")
	}

	for i := 0; i < len(tracker.results)+1; i++ {
		tracker.ReportResult(false)
	}
	if tracker.failures != uint(len(tracker.results)) {
		t.Errorf("It should not be possible for failures to go above the tracker size. Got: %d", tracker.failures)
	}
}
