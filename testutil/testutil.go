package testutil

import (
	"database/sql"
	"sync"
)

type QueryCounterDBMock struct {
	*sql.DB
	ecount uint
	qcount uint
	mux    sync.Mutex
}

func NewQueryCounterDBMock() *QueryCounterDBMock {
	return &QueryCounterDBMock{}
}

func (db *QueryCounterDBMock) Exec(query string, args ...interface{}) (sql.Result, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.ecount++
	return nil, nil
}

func (db *QueryCounterDBMock) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.qcount++
	return nil, nil
}

func (db *QueryCounterDBMock) GetQueryCount() uint {
	return db.qcount
}

func (db *QueryCounterDBMock) GetExecCount() uint {
	return db.ecount
}

func (db *QueryCounterDBMock) Ping() error {
	return nil
}
