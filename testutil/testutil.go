package testutil

import (
	"database/sql"
	"errors"
	"sync"
)

type QueryCounterDBMock struct {
	*sql.DB
	ecount uint
	qcount uint
	mux    sync.Mutex
}

type AlwaysFailsDBMock struct {
	*sql.DB
	qcount uint
	mux    sync.Mutex
}

type FailsNTimesDBMock struct {
	*sql.DB
	failtimes uint
	ecount    uint
	qcount    uint
	mux       sync.Mutex
}

type AlwaysClosedDBMock struct {
	*sql.DB
}

func NewQueryCounterDBMock() *QueryCounterDBMock {
	return &QueryCounterDBMock{}
}

func NewAlwaysFailsDBMock() *AlwaysFailsDBMock {
	return &AlwaysFailsDBMock{}
}

func NewFailsNTimesDBMock(failtimes uint) *FailsNTimesDBMock {
	return &FailsNTimesDBMock{failtimes: failtimes}
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

func (db *AlwaysFailsDBMock) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	db.qcount++
	return nil, errors.New("I am an errror")
}

func (db *AlwaysFailsDBMock) GetQueryCount() uint {
	return db.qcount
}

func (db *AlwaysFailsDBMock) Ping() error {
	return errors.New("The DB that only fails has failed")
}

func (db *FailsNTimesDBMock) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	if db.qcount < db.failtimes {
		db.qcount++
		return nil, errors.New("If at first you don't succeed...")
	} else {
		db.qcount++
		return nil, nil
	}
}

func (db *FailsNTimesDBMock) Exec(query string, args ...interface{}) (sql.Result, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	if db.ecount < db.failtimes {
		db.ecount++
		return nil, errors.New("If at first you don't succeed...")
	} else {
		db.ecount++
		return nil, nil
	}
}

func (db *FailsNTimesDBMock) GetQueryCount() uint {
	return db.qcount
}

func (db *FailsNTimesDBMock) GetExecCount() uint {
	return db.ecount
}

func (db *FailsNTimesDBMock) Ping() error {
	return errors.New("The DB that only fails has failed")
}
