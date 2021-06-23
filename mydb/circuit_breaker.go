package mydb

import (
	"context"
	"database/sql"
	"sync"
	"time"

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
	db      DatabaseClient
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

func (m *masterCBDB) ApplyConfig(config DBConfig) {
	m.tracker.SetWindowSize(int(config.CBResultWindowSize))
}

func (r *replicaCBDB) ApplyConfig(config DBConfig) {
	r.tracker.SetWindowSize(int(config.CBResultWindowSize))
}

// GetIfReady will return a reference to a Circuit Breaking clients if if it is closed
// it returns half-open half the time, Ping()-ing the other half the time
// an open client will Ping() and skip
func (r *replicaCBDB) GetIfReady() *replicaCBDB {
	r.mux.Lock()
	defer r.mux.Unlock()
	if r.tracker.Status() == statustracker.Closed {
		return r
	} else if r.tracker.Status() == statustracker.HalfOpen && r.skip == 0 {
		r.skip = 1
		return r
	} else if r.tracker.Status() == statustracker.HalfOpen {
		go r.Ping()
		r.skip = 0
		return nil
	}
	go r.Ping()
	return nil
}

func (r *replicaCBDB) Ping() error {
	e := r.db.Ping()
	r.tracker.ReportStatus(e == nil)
	return e
}

func (r *replicaCBDB) PingContext(ctx context.Context) error {
	e := r.db.PingContext(ctx)
	r.tracker.ReportStatus(e == nil)
	return e
}

func (r *replicaCBDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := r.db.Query(query, args...)
	r.tracker.ReportStatus(e == nil)
	return rows, e
}

func (r *replicaCBDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := r.db.QueryContext(ctx, query, args...)
	r.tracker.ReportStatus(e == nil)
	return rows, e
}

func (r *replicaCBDB) QueryRow(query string, args ...interface{}) *sql.Row {
	row := r.db.QueryRow(query, args...)
	r.tracker.ReportStatus(row.Err() == nil)
	return row
}

func (r *replicaCBDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows := r.db.QueryRowContext(ctx, query, args...)
	r.tracker.ReportStatus(rows.Err() == nil)
	return rows
}

func (r *replicaCBDB) Begin() (*sql.Tx, error) {
	tx, e := r.db.Begin()
	r.tracker.ReportStatus(e == nil)
	return tx, e
}

func (r *replicaCBDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, e := r.db.BeginTx(ctx, opts)
	r.tracker.ReportStatus(e == nil)
	return tx, e
}

func (r *replicaCBDB) Close() error {
	return r.db.Close()
}

func (r *replicaCBDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	result, e := r.db.Exec(query, args...)
	r.tracker.ReportStatus(e == nil)
	return result, e
}

func (r *replicaCBDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, e := r.db.ExecContext(ctx, query, args...)
	r.tracker.ReportStatus(e == nil)
	return result, e
}

func (r *replicaCBDB) Prepare(query string) (*sql.Stmt, error) {
	stmt, e := r.db.Prepare(query)
	r.tracker.ReportStatus(e == nil)
	return stmt, e
}

func (r *replicaCBDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	stmt, e := r.db.PrepareContext(ctx, query)
	r.tracker.ReportStatus(e == nil)
	return stmt, e
}

func (r *replicaCBDB) SetConnMaxLifetime(d time.Duration) {
	r.db.SetConnMaxLifetime(d)
}

func (r *replicaCBDB) SetMaxIdleConns(n int) {
	r.db.SetMaxIdleConns(n)
}

func (r *replicaCBDB) SetMaxOpenConns(n int) {
	r.db.SetMaxOpenConns(n)
}
