package mydb

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/m-rec/08647c57124934494b415428d23b56b52f043339/testutil"
)

// The following statement is left intact
// 1. Because I was told to
// 2. As long as this compiles, it means mydb's
//    'DatabaseClient' interface will accept database/sql.DB's

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

func newMyDBCounterMock(numreplicas int) (*DB, *testutil.QueryCounterDBMock, []*testutil.QueryCounterDBMock) {
	masterdb := testutil.NewQueryCounterDBMock()
	var asInterface []DatabaseClient
	var replicas []*testutil.QueryCounterDBMock
	for i := 0; i < numreplicas; i++ {
		mock := testutil.NewQueryCounterDBMock()
		asInterface = append(asInterface, DatabaseClient(mock))
		replicas = append(replicas, mock)
	}
	return newWithGeneric(masterdb, asInterface...), masterdb, replicas
}

func newWithGeneric(master DatabaseClient, readreplicas ...DatabaseClient) *DB {
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

func TestExecQueriesMaster(t *testing.T) {
	db, master, _ := newMyDBCounterMock(3)
	_, _ = db.Exec("UPDATE very_important_business_documents")
	if master.GetExecCount() != 1 {
		t.Errorf("Expected master to recieve 1 exec but it did not")
		t.Fail()
	}
}

func TestExecSuccessOnRetryReturnsSuccessful(t *testing.T) {
	failtimes := 1
	master := testutil.NewFailsNTimesDBMock(uint(failtimes))
	db := newWithGeneric(master)
	_, err := db.Exec("UPDATE very_important_business_documents")
	if err != nil {
		t.Errorf("Expected a success")
		t.FailNow()
	}
	if master.GetExecCount() != 2 {
		t.Errorf("Expected master to recieve 1 exec but it did not")
		t.FailNow()
	}
}

func TestReadQueriesReplica0(t *testing.T) {
	db, _, replicas := newMyDBCounterMock(1)

	_, _ = db.Query("SELECT plaintext_passwords")
	if replicas[0].GetQueryCount() != 1 {
		t.Errorf("Expected Replica 1 to recieve 1 query but it did not")
		t.Fail()
	}
}

func TestReadQueriesLoadBalance(t *testing.T) {
	db, _, replicas := newMyDBCounterMock(3)
	for range replicas {
		_, _ = db.Query("SELECT plaintext_passwords")
	}
	for i, mock := range replicas {
		if mock.GetQueryCount() != 1 {
			t.Errorf("Replica %d expected to receive a query but didn't.\n", i+1)
			t.Fail()
		}
	}
}

func TestLoadBalancingCycles(t *testing.T) {
	db, _, replicas := newMyDBCounterMock(3)
	for range replicas {
		_, _ = db.Query("SELECT plaintext_passwords")
	}
	_, _ = db.Query("SELECT user_private_keys")
	if replicas[0].GetQueryCount() != 2 {
		t.Error("Replica 1 expected to receive 2 queries but didn't.\n")
		t.FailNow()
	}
}

func TestReadLoadBalancingIsCoordinated(t *testing.T) {
	replicact, expected := 5, 3
	db, _, replicas := newMyDBCounterMock(replicact)
	var wg sync.WaitGroup
	wg.Add(replicact * expected)
	for i := 0; i < replicact*expected; i++ {
		go func() {
			defer wg.Done()
			_, _ = db.Query("SELECT plaintext_passwords")
		}()
	}
	wg.Wait()
	for i, mock := range replicas {
		if mock.GetQueryCount() != uint(expected) {
			t.Errorf("Replica %d expected %d requests but recieved %d\n", i+1, expected, mock.GetQueryCount())
			t.Fail()
		}
	}
}

func TestReplicasRetry(t *testing.T) {
	replica1 := testutil.NewAlwaysFailsDBMock()
	replica2 := testutil.NewQueryCounterDBMock()
	db := newWithGeneric(nil, replica1, replica2)
	_, _ = db.Query("SELECT very_important_business_documents")
	if replica1.GetQueryCount() != 1 {
		t.Error("The first replica should have been attempted")
		t.FailNow()
	}
	if replica2.GetQueryCount() != 1 {
		t.Errorf("The second replica should have been queries")
		t.FailNow()
	}
}

func TestQuerySuccessOnRetryReturnsSuccessful(t *testing.T) {
	failtimes := 1
	replica1 := testutil.NewFailsNTimesDBMock(uint(failtimes))
	replica2 := testutil.NewAlwaysFailsDBMock()
	db := newWithGeneric(nil, replica1, replica2)
	_, err := db.Query("SELECT very_important_business_documents")
	if err != nil {
		t.Error("Should have eventually gotten a non-nil result")
		t.FailNow()
	}
	if replica2.GetQueryCount() != 1 {
		t.Error("The first replica should have been attempted")
		t.FailNow()
	}
	actual := replica1.GetQueryCount()
	if actual != uint(failtimes)+1 {
		t.Errorf("The second replica should have been queries %d times but was queried %d times", failtimes+1, actual)
		t.FailNow()
	}
}

func TestMasterRecieveQueryWhenReadReplicasOpen(t *testing.T) {
	master := testutil.NewQueryCounterDBMock()
	db := newWithGeneric(master, testutil.NewAlwaysFailsDBMock(), testutil.NewAlwaysFailsDBMock())

	// Since read replicas only fail, this config will force them all to to retry until
	// they reach the 'Open' circuit breaker state
	err := db.ApplyConfig(
		DBConfig{
			CBResultWindowSize:  3,
			CBHalfOpenThreshold: 1,
			CBOpenThreshold:     2,
			MaxAttempts:         100,
		},
	)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = db.Query("UPDATE very_important_business_documents")
	if err != nil {
		t.Error("Expected success")
		t.FailNow()
	}
	if master.GetQueryCount() != 1 {
		t.Error("Master should have received the query")
		t.FailNow()
	}
}
