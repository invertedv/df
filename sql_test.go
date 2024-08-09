package df

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

// NewConnect established a new connection to ClickHouse.
// host is IP address (assumes port 9000), memory is max_memory_usage
func newConnect(host, user, password string) (db *sql.DB, err error) {
	db = clickhouse.OpenDB(
		&clickhouse.Options{
			Addr: []string{host + ":9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: user,
				Password: password,
			},
			DialTimeout: 300 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
				Level:  0,
			},
		})

	return db, db.Ping()
}

func df4test() (*SQLdf, error) {
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db, e := newConnect(host, user, password)
	if e != nil {
		return nil, e
	}

	df, e1 := NewSQLdf("SELECT * FROM econGo.final", db)
	if e1 != nil {
		return nil, e1
	}

	return df, nil
}

func TestNewSQLdf(t *testing.T) {
	df, e := df4test()
	assert.Nil(t, e)
	defer func() { _ = df.db.Close() }()

	f := SQLfunctions["add"]
	col, e := df.Column("cbsa")
	assert.Nil(t, e)
	col1, e1 := df.Column("q25")
	assert.Nil(t, e1)
	_, _ = col, col1

	e1 = df.Apply("test", SQLrun, f, "q10", "q25")
	assert.Nil(t, e1)

	r, e2 := df.Column("test")
	assert.Nil(t, e2)
	_ = r
	_ = df

}
