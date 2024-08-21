package df

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/df"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, -1})
	y, _ := NewMemCol("y", []int{4, 5, 6, 1, 4, 4})
	z, _ := NewMemCol("z", []string{"p20221231", "20000101", "19900615", "20220601", "20230915", "20060310"})
	df, _ := NewMemDF(Run, StandardFunctions(), x, y, z)

	return df
}

func TestDF_Apply(t *testing.T) {
	df := makeMemDF()
	col, e := df.Column("z")
	assert.Nil(t, e)
	e1 := df.Apply("test", "cast", "DTstring", "z")
	assert.Nil(t, e1)
	c1, _ := df.Column("test")
	fmt.Println(c1.Data())
	//	assert.Nil(t, e1)
	col, e = df.Column("x")
	assert.Nil(t, e)
	col1, e1 := df.Column("y")
	_, _ = col, col1
	assert.Nil(t, e1)
	e1 = df.Apply("test1", "add", "x", "y")
	fmt.Println(df.ColumnNames())
	fmt.Println(df.ColumnCount())
	c, _ := df.Column("test1")
	fmt.Println(c.Data())
	e1 = df.Apply("xyz", "aaa", "z")
	assert.Nil(t, e1)
	e1 = df.DropColumns("test1")
	assert.Nil(t, e1)
}

func TestDF_Sort(t *testing.T) {
	df := makeMemDF()
	e := df.Sort("y", "z")
	assert.Nil(t, e)
	x, _ := df.Column("x")
	y, _ := df.Column("y")
	z, _ := df.Column("z")
	fmt.Println(x, y, z)
}

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

func TestLoadSQL(t *testing.T) {
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")

	var (
		db *sql.DB
		e  error
	)

	db, e = newConnect(host, user, password)
	assert.Nil(t, e)

	var dialect *df.Dialect
	dialect, e = df.NewDialect("clickhouse", db)
	assert.Nil(t, e)
	memDF, e1 := LoadDB("SELECT * FROM zip.zip3 LIMIT 10", dialect)
	assert.Nil(t, e1)
	col, e2 := memDF.Column("prop_zip3")
	assert.Nil(t, e2)
	fmt.Println(col.Data())

	ed := memDF.CreateTable("tmp.aaa", "prop_zip3", true, "prop_zip3", "latitude")
	assert.Nil(t, ed)
	a := db.Stats()
	_ = a
}
