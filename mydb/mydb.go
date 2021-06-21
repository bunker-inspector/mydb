package mydb

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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

type DBConfig struct {
	ConnectionTimeout time.Duration
}

type DB struct {
	master       DatabaseClient
	readreplicas []DatabaseClient
	config       DBConfig
	replicamutex sync.Mutex
}

func New(master *sql.DB, readreplicas ...*sql.DB) *DB {
	// The motivation for the following is that in real world cases, a sql.DB would be supplied
	// and we want to prove that check at compile time, which the go compiler would not allow
	// where it defined as []DatabseClient but we want to be able to apply simple mocks for
	// testing and in the scope of this library, we aren't interested in the actual backend
	var convertedReplicas []DatabaseClient
	for _, replica := range readreplicas {
		convertedReplicas = append(convertedReplicas, DatabaseClient(replica))
	}
	db := &DB{
		master: DatabaseClient(master),
		config: DBConfig{
			ConnectionTimeout: 5 * time.Second,
		},
		readreplicas: convertedReplicas,
	}
	return db
}

// WithConfig sets the config for the client
func (db *DB) WithConfig(config DBConfig) *DB {
	db.config = config
	return db
}

func (db *DB) readReplicaRoundRobin() DatabaseClient {
	db.replicamutex.Lock()
	defer db.replicamutex.Unlock()

	if len(db.readreplicas) == 0 {
		// Use master to read if no replicas are available
		return db.master
	}

	next := db.readreplicas[0]
	db.readreplicas = append(db.readreplicas[1:], next)
	return next
}

func (db *DB) Ping() error {
	if err := db.master.Ping(); err != nil {
		panic(err)
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].Ping(); err != nil {
			panic(err)
		}
	}

	return nil
}

func (db *DB) PingContext(ctx context.Context) error {
	if err := db.master.PingContext(ctx); err != nil {
		logrus.Warn("Master instance unavailable. Attempting to reconnect...\n")
	}

	for i := range db.readreplicas {
		if err := db.readreplicas[i].PingContext(ctx); err != nil {
			logrus.Warnf("Replica instance %d unavailable. Attempting to reconnect...\n", i)
		}
	}

	return nil
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.readReplicaRoundRobin().Query(query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.readReplicaRoundRobin().QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.readReplicaRoundRobin().QueryRow(query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.readReplicaRoundRobin().QueryRowContext(ctx, query, args...)
}

func (db *DB) Begin() (*sql.Tx, error) {
	return db.master.Begin()
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

func (db *DB) Close() error {
	db.master.Close()
	for _, replica := range db.readreplicas {
		replica.Close()
	}
	return nil
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.master.Exec(query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.master.Prepare(query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.master.PrepareContext(ctx, query)
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.master.SetConnMaxLifetime(d)
	for i := range db.readreplicas {
		db.readreplicas[i].SetConnMaxLifetime(d)
	}
}

func (db *DB) SetMaxIdleConns(n int) {
	db.master.SetMaxIdleConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxIdleConns(n)
	}
}

func (db *DB) SetMaxOpenConns(n int) {
	db.master.SetMaxOpenConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxOpenConns(n)
	}
}
