package mydb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/m-rec/08647c57124934494b415428d23b56b52f043339/testutil"
)

// DO NOT EDIT this assign statement.
var _ interface {
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
} = New((*sql.DB)(nil), []*sql.DB{}...)

func newMyDBMock(numreplicas int) (*DB, sqlmock.Sqlmock, []sqlmock.Sqlmock) {
	masterdb, mastermock, _ := sqlmock.New()
	var replicas []*sql.DB
	var replicamocks []sqlmock.Sqlmock
	for i := 0; i < numreplicas; i++ {
		db, mock, _ := sqlmock.New()
		replicas = append(replicas, db)
		replicamocks = append(replicamocks, mock)
	}
	return New(masterdb, replicas...), mastermock, replicamocks
}

func newWithGeneric(master DatabaseClient, readreplicas ...DatabaseClient) *DB {
	db := &DB{
		master: master,
		config: DBConfig{
			ConnectionTimeout: 5 * time.Second,
		},
		readreplicas: readreplicas,
	}
	return db
}

func TestExecQueriesMaster(t *testing.T) {
	db, mastermock, _ := newMyDBMock(3)
	mastermock.ExpectExec("UPDATE very_important_business_documents")

	_, _ = db.Exec("UPDATE very_important_business_documents")
	if e := mastermock.ExpectationsWereMet(); e != nil {
		t.Errorf("Failed :%s", e)
		t.Fail()
	}
}

func TestReadQueriesReplica0(t *testing.T) {
	db, _, replicamocks := newMyDBMock(1)
	replicamocks[0].ExpectQuery("SELECT plaintext_passwords")

	_, _ = db.Query("SELECT plaintext_passwords")
	if e := replicamocks[0].ExpectationsWereMet(); e != nil {
		t.Errorf("Failed :%s", e)
		t.Fail()
	}
}

func TestReadQueriesLoadBalance(t *testing.T) {
	db, _, replicamocks := newMyDBMock(3)
	for _, mocks := range replicamocks {
		mocks.ExpectQuery("SELECT plaintext_passwords")
	}
	for range replicamocks {
		_, _ = db.Query("SELECT plaintext_passwords")
	}
	for i, mock := range replicamocks {
		if e := mock.ExpectationsWereMet(); e != nil {
			t.Errorf("Replica %d expected to receive a query but didn't.\n", i+1)
			t.Fail()
		}
	}
}

func TestLoadBalancingCycles(t *testing.T) {
	db, _, replicamocks := newMyDBMock(3)
	replicamocks[0].ExpectQuery("SELECT plaintext_passwords")
	replicamocks[0].ExpectQuery("SELECT user_private_keys")
	for range replicamocks {
		_, _ = db.Query("SELECT plaintext_passwords")
	}
	_, _ = db.Query("SELECT user_private_keys")
	if e := replicamocks[0].ExpectationsWereMet(); e != nil {
		t.Error("Replica 1 expected to receive 2 queries but recieved none.\n")
		t.FailNow()
	}
}

func TestReadLoadBalancingIsThreadSafe(t *testing.T) {
	replicact, expected := 5, 3
	var counters []DatabaseClient
	for i := 0; i < replicact; i++ {
		counters = append(counters, testutil.NewQueryCounter())
	}
	db := newWithGeneric(&sql.DB{}, counters...)
	var wg sync.WaitGroup
	wg.Add(replicact * expected)
	for i := 0; i < replicact*expected; i++ {
		go func() {
			defer wg.Done()
			_, _ = db.Query("SELECT plaintext_passwords")
		}()
	}
	wg.Wait()
	for i, iface := range counters {
		count := iface.(*testutil.QueryCounterDBMock)
		if count.GetCount() != uint(expected) {
			t.Errorf("Replica %d expected %d requests but recieved %d\n", i+1, expected, count.GetCount())
			t.Fail()
		}
	}
}
