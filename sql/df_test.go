package sql

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	df2 "github.com/invertedv/df"

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

	// , ln_zb_dt
	//	df, e1 := NewSQLdf("SELECT ln_id, vintage, ln_orig_ir, last_dt FROM fannie.final limit 10000", df2.NewContext(dialect, nil, nil))
	df, e1 := NewSQLdf("SELECT * FROM zip.zip3 LIMIT 10", df2.NewContext(dialect, nil, nil))
	if e1 != nil {
		return nil, e1
	}

	return df, nil
}

func TestNewSQLdf(t *testing.T) {
	df, e := df4test()
	assert.Nil(t, e)
	defer func() { _ = df.Context.Dialect().DB().Close() }()

	e = df.Apply("test1", "c", "DTint", "1")
	assert.Nil(t, e)

	e = df.Apply("test2", "cast", "DTstring", "latitude")
	assert.Nil(t, e)

	e2 := df.Apply("c", "add", "10", "latitude")
	assert.Nil(t, e2)

	e2 = df.Apply("d", "abs", "latitude")
	assert.Nil(t, e2)

	e2 = df.Apply("e", "exp", "1.1")
	assert.Nil(t, e2)

	//	e = df.Apply("test1", "cast", "DTstring", "latitude")
	//	assert.Nil(t, e)

	e1 := df.Apply("test", "add", "latitude", "longitude")
	assert.Nil(t, e1)

	_, e2 = df.Column("test")
	assert.Nil(t, e2)

	fmt.Println(df.RowCount())
	fmt.Println(df.MakeQuery())
	r := df.MakeQuery()
	_ = r
	dx, _ := df.Column("latitude")
	_ = dx
	e = df.CreateTable("tmp.aaa", "prop_zip3", true)
	assert.Nil(t, e)
	e = df.DBsave("tmp.aaa", true)
	assert.Nil(t, e)

	fmt.Println(df.RowCount())
	e = df.FileSave("/home/will/tmp/test.csv")
	assert.Nil(t, e)

}
