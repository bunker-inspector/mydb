package mydb

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/bunker-inspector/mydb/statustracker"
)

// masterCBDB is a circuit breaking DatabaseClient
// that knows it is not a member of a group
type masterCBDB struct {
	db      DatabaseClient
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
	m.tracker.SetThresholds(config.CBHalfOpenThreshold, config.CBOpenThreshold)
}

func (r *replicaCBDB) ApplyConfig(config DBConfig) {
	r.tracker.SetWindowSize(int(config.CBResultWindowSize))
	r.tracker.SetThresholds(config.CBHalfOpenThreshold, config.CBOpenThreshold)
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
	} else if r.skip == 1 {
		go r.Ping()
		r.skip = 0
	} else {
		r.skip = 1
	}
	return nil
}

// Master Circuit Breaker impl

func (m *masterCBDB) Ping() error {
	e := m.db.Ping()
	m.tracker.ReportResult(e == nil)
	return e
}

func (m *masterCBDB) PingContext(ctx context.Context) error {
	e := m.db.PingContext(ctx)
	m.tracker.ReportResult(e == nil)
	return e
}

func (m *masterCBDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := m.db.Query(query, args...)
	m.tracker.ReportResult(e == nil)
	return rows, e
}

func (m *masterCBDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := m.db.QueryContext(ctx, query, args...)
	m.tracker.ReportResult(e == nil)
	return rows, e
}

func (m *masterCBDB) QueryRow(query string, args ...interface{}) *sql.Row {
	row := m.db.QueryRow(query, args...)
	m.tracker.ReportResult(row.Err() == nil)
	return row
}

func (m *masterCBDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows := m.db.QueryRowContext(ctx, query, args...)
	m.tracker.ReportResult(rows.Err() == nil)
	return rows
}

func (m *masterCBDB) Begin() (*sql.Tx, error) {
	tx, e := m.db.Begin()
	m.tracker.ReportResult(e == nil)
	return tx, e
}

func (m *masterCBDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, e := m.db.BeginTx(ctx, opts)
	m.tracker.ReportResult(e == nil)
	return tx, e
}

func (m *masterCBDB) Close() error {
	return m.db.Close()
}

func (m *masterCBDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	result, e := m.db.Exec(query, args...)
	m.tracker.ReportResult(e == nil)
	return result, e
}

func (m *masterCBDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, e := m.db.ExecContext(ctx, query, args...)
	m.tracker.ReportResult(e == nil)
	return result, e
}

func (m *masterCBDB) Prepare(query string) (*sql.Stmt, error) {
	stmt, e := m.db.Prepare(query)
	m.tracker.ReportResult(e == nil)
	return stmt, e
}

func (m *masterCBDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	stmt, e := m.db.PrepareContext(ctx, query)
	m.tracker.ReportResult(e == nil)
	return stmt, e
}

func (m *masterCBDB) SetConnMaxLifetime(d time.Duration) {
	m.db.SetConnMaxLifetime(d)
}

func (m *masterCBDB) SetMaxIdleConns(n int) {
	m.db.SetMaxIdleConns(n)
}

func (m *masterCBDB) SetMaxOpenConns(n int) {
	m.db.SetMaxOpenConns(n)
}

// Replica Circuit Breaker impl

func (r *replicaCBDB) Ping() error {
	e := r.db.Ping()
	r.tracker.ReportResult(e == nil)
	return e
}

func (r *replicaCBDB) PingContext(ctx context.Context) error {
	e := r.db.PingContext(ctx)
	r.tracker.ReportResult(e == nil)
	return e
}

func (r *replicaCBDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := r.db.Query(query, args...)
	r.tracker.ReportResult(e == nil)
	return rows, e
}

func (r *replicaCBDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, e := r.db.QueryContext(ctx, query, args...)
	r.tracker.ReportResult(e == nil)
	return rows, e
}

func (r *replicaCBDB) QueryRow(query string, args ...interface{}) *sql.Row {
	row := r.db.QueryRow(query, args...)
	r.tracker.ReportResult(row.Err() == nil)
	return row
}

func (r *replicaCBDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows := r.db.QueryRowContext(ctx, query, args...)
	r.tracker.ReportResult(rows.Err() == nil)
	return rows
}

func (r *replicaCBDB) Begin() (*sql.Tx, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
}

func (r *replicaCBDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
}

func (r *replicaCBDB) Close() error {
	return r.db.Close()
}

func (r *replicaCBDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
}

func (r *replicaCBDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
}

func (r *replicaCBDB) Prepare(query string) (*sql.Stmt, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
}

func (r *replicaCBDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, errors.New("Replicas cannot perform write operations. Please check your configuration.")
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
