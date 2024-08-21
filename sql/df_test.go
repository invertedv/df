package sql

import (
	"database/sql"
	"fmt"
	df2 "github.com/invertedv/df"
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

	var (
		db *sql.DB
		e  error
	)

	if db, e = newConnect(host, user, password); e != nil {
		return nil, e
	}

	var dialect *df2.Dialect
	dialect, e = df2.NewDialect("clickhouse", db)

	df, e1 := NewSQLdf("SELECT * FROM zip.zip3 LIMIT 10", dialect)
	if e1 != nil {
		return nil, e1
	}

	return df, nil
}

func TestNewSQLdf(t *testing.T) {
	df, e := df4test()
	assert.Nil(t, e)
	defer func() { _ = df.DB().Close() }()

	/*	col, e := df.Column("cbsa")
		assert.Nil(t, e)
		col1, e1 := df.Column("q25")
		assert.Nil(t, e1)
		_, _ = col, col1


	*/
	e1 := df.Apply("test", "add", "latitude", "longitude")
	assert.Nil(t, e1)
	e = df.Apply("test1", "cast", "DTstring", "latitude")
	assert.Nil(t, e)

	_, e2 := df.Column("test")
	assert.Nil(t, e2)

	fmt.Println(df.RowCount())
	fmt.Println(df.MakeQuery())
	e = df.CreateTable("tmp.aaa", "prop_zip3", true, "prop_zip3")
	assert.Nil(t, e)
	e = df.DBsave("tmp.aaa", true)
	assert.Nil(t, e)

	fmt.Println(df.RowCount())

}
