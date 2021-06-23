package mydb

import (
	"sync"

	"github.com/m-rec/08647c57124934494b415428d23b56b52f043339/statustracker"
)

// masterCBDB is a circuit breaking DatabaseClient
// that knows it is not a member of a group
type masterCBDB struct {
	DatabaseClient
	tracker *statustracker.StatusTracker
	skip    uint
	mux     sync.Mutex
}

// replicaCBDB is a circuit breaking DatabaseClient
// that knows it has group members for failover
type replicaCBDB struct {
	DatabaseClient
	tracker *statustracker.StatusTracker
	skip    uint
	mux     sync.Mutex
}

func newMasterCBDB(db DatabaseClient) *masterCBDB {
	return &masterCBDB{db, statustracker.NewStatusTracker(), 0, sync.Mutex{}}
}

func newReplicaCBDB(db DatabaseClient) *replicaCBDB {
	return &replicaCBDB{db, statustracker.NewStatusTracker(), 0, sync.Mutex{}}
}

func (db *masterCBDB) ApplyConfig(config DBConfig) {
	db.tracker.SetWindowSize(int(config.CBResultWindowSize))
}

func (db *replicaCBDB) ApplyConfig(config DBConfig) {
	db.tracker.SetWindowSize(int(config.CBResultWindowSize))
}

// GetIfReady will return a reference to a Circuit Breaking clients if if it is closed
// it returns half-open half the time, Ping()-ing the other half the time
// an open client will Ping() and skip
func (db *replicaCBDB) GetIfReady() *replicaCBDB {
	db.mux.Lock()
	defer db.mux.Unlock()
	if db.tracker.Status() == statustracker.Closed {
		return db
	} else if db.tracker.Status() == statustracker.HalfOpen && db.skip == 0 {
		db.skip = 1
		return db
	} else if db.tracker.Status() == statustracker.HalfOpen {
		go db.Ping()
		db.skip = 0
		return nil
	}
	go db.Ping()
	return nil
}
