// Package mydb provides a load-balancing, error tolerant wrapper around
// the standard sql.DB
package mydb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type DatabaseClient interface {
	Ping() error
	PingContext(ctx context.Context) error
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
}

// The DB config provides options for configuring the circuit breaking behavior
// and the master instance backoff parameters
type DBConfig struct {
	// How many events a circuit breaker will keep in its result history
	// to determine its status
	CBResultWindowSize uint

	// Threshold at which circuit breakers will enter the half-open state
	CBHalfOpenThreshold uint

	// Threshold at at which circuit breakers will enter the open state
	CBOpenThreshold uint

	// Max number of attempts
	MaxAttempts uint

	// The rate at which the master retry wait times increase
	// in the event of repeat failures.
	// Will be 0 * x, 1 * x, ..., MaxAttempts-1 * x milliseconds
	// where x is this value
	MasterBackoffFactor time.Duration
}

// DB is the facade that manages load balancing reads and retries
type DB struct {
	master       *masterCBDB
	replicas     []*replicaCBDB
	config       DBConfig
	replicamutex sync.Mutex
}

func New(master *sql.DB, readreplicas ...*sql.DB) *DB {
	var convertedReplicas []*replicaCBDB
	for _, replica := range readreplicas {
		asCircuitBreaker := newReplicaCBDB(replica)
		convertedReplicas = append(convertedReplicas, asCircuitBreaker)
	}
	db := &DB{
		master: newMasterCBDB(master),
		config: DBConfig{
			MasterBackoffFactor: 2000 * time.Millisecond,
			MaxAttempts:         3,
		},
		replicas: convertedReplicas,
	}
	return db
}

// ApplyConfig sets the config for the client.
func (db *DB) ApplyConfig(config DBConfig) error {
	if config.CBResultWindowSize <= 2 {
		return fmt.Errorf("Cannot apply configuration. The minumum window size is 2 (which is still not really useful)")
	}
	if config.CBOpenThreshold > config.CBResultWindowSize {
		return fmt.Errorf("Cannot apply configuration. The result window size must be at least equal to the open threshold.")
	}
	if config.CBHalfOpenThreshold >= config.CBOpenThreshold {
		return fmt.Errorf("Cannot apply configuration. The open must be greater than the half-open threshold.")
	}

	db.config = config
	db.master.ApplyConfig(config)

	for _, replica := range db.replicas {
		replica.ApplyConfig(config)
	}

	return nil
}

// readReplicaRoundRobin will attempt to select the first read replica to report itself
// as available. If none are found, the master instance will be selected
func (db *DB) readReplicaRoundRobin() DatabaseClient {
	db.replicamutex.Lock()
	defer db.replicamutex.Unlock()

	// take first ready replica
	for i := 0; i < len(db.replicas); i++ {
		if replica := db.replicas[0].GetIfReady(); replica != nil {
			db.replicas = append(db.replicas[1:], db.replicas[0])
			return replica
		}
		db.replicas = append(db.replicas[1:], db.replicas[0])
	}

	// Use master to read if no replicas are available
	return db.master
}

func (db *DB) Ping() error {
	var err error
	if pingerr := db.master.Ping(); err != nil {
		err = multierr.Append(err, pingerr)
		logrus.Warn("Master instance unavailable. Attempting to reconnect...\n")
	}

	for i := range db.replicas {
		if pingerr := db.replicas[i].Ping(); err != nil {
			err = multierr.Append(err, pingerr)
			logrus.Warnf("Replica instance %d unavailable. Attempting to reconnect...\n", i)
		}
	}

	return err
}

func (db *DB) PingContext(ctx context.Context) error {
	var err error
	if pingerr := db.master.PingContext(ctx); err != nil {
		err = multierr.Append(err, pingerr)
		logrus.Warn("Master instance unavailable. Attempting to reconnect...\n")
	}

	for i := range db.replicas {
		if pingerr := db.replicas[i].PingContext(ctx); err != nil {
			err = multierr.Append(err, pingerr)
			logrus.Warnf("Replica instance %d unavailable. Attempting to reconnect...\n", i)
		}
	}

	return nil
}

func (db *DB) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		rows, err = db.readReplicaRoundRobin().Query(query, args...)
		if err == nil {
			return rows, nil
		}
	}
	return nil, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		rows, err = db.readReplicaRoundRobin().QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}
	}
	return nil, err

}

func (db *DB) QueryRow(query string, args ...interface{}) (row *sql.Row) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		row = db.readReplicaRoundRobin().QueryRow(query, args...)
		if row.Err() == nil {
			return row
		}
	}
	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sql.Row) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		row = db.readReplicaRoundRobin().QueryRowContext(ctx, query, args...)
		if row.Err() == nil {
			return row
		}
	}
	return row
}

func (db *DB) Begin() (tx *sql.Tx, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		tx, err = db.master.Begin()
		if err == nil {
			return tx, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (tx *sql.Tx, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		tx, err = db.master.BeginTx(ctx, opts)
		if err == nil {
			return tx, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) Close() error {
	db.master.Close()
	for _, replica := range db.replicas {
		replica.Close()
	}
	return nil
}

func (db *DB) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		res, err = db.master.Exec(query, args...)
		if err == nil {
			return res, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		res, err = db.master.ExecContext(ctx, query, args...)
		if err == nil {
			return res, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) Prepare(query string) (stmt *sql.Stmt, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		stmt, err = db.master.Prepare(query)
		if err == nil {
			return stmt, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	for i := 0; i < int(db.config.MaxAttempts); i++ {
		stmt, err = db.master.PrepareContext(ctx, query)
		if err == nil {
			return stmt, nil
		}
		time.Sleep(time.Duration(i) * db.config.MasterBackoffFactor)
	}
	return nil, err
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.master.SetConnMaxLifetime(d)
	for i := range db.replicas {
		db.replicas[i].SetConnMaxLifetime(d)
	}
}

func (db *DB) SetMaxIdleConns(n int) {
	db.master.SetMaxIdleConns(n)
	for i := range db.replicas {
		db.replicas[i].SetMaxIdleConns(n)
	}
}

func (db *DB) SetMaxOpenConns(n int) {
	db.master.SetMaxOpenConns(n)
	for i := range db.replicas {
		db.replicas[i].SetMaxOpenConns(n)
	}
}
