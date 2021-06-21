package testutil

import (
	"database/sql"
	"sync"
)

type QueryCounterDBMock struct {
	*sql.DB
	count uint
	mux   sync.Mutex
}

func NewQueryCounter() *QueryCounterDBMock {
	return &QueryCounterDBMock{}
}

func (db *QueryCounterDBMock) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.count++
	return nil, nil
}

func (db *QueryCounterDBMock) GetCount() uint {
	return db.count
}
