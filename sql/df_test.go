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
	df, e1 := NewSQLdf("SELECT *, toInt32(prop_zip3='005') AS logical1 FROM zip.zip3 LIMIT 10", df2.NewContext(dialect, nil, nil))
	if e1 != nil {
		return nil, e1
	}

	return df, nil
}
func TestWhere(t *testing.T) {
	dfx, e := df4test()
	assert.Nil(t, e)
	expr := "where(latitude<=42.5 && longitude > -73.0)"
	out, e := dfx.Parse(expr)
	assert.Nil(t, e)
	outDF := out.AsDF().(*SQLdf)
	//assert.Equal(t, 10, outDF.AsDF().RowCount())
	fmt.Println(outDF.MakeQuery())
	e = outDF.CreateTable("tmp.aaa", "prop_zip3", true)
	assert.Nil(t, e)
	e = outDF.DBsave("tmp.aaa", true)
	assert.Nil(t, e)

	/*	outDF, e = dfx.Parse("where(x >= 1.0)")
		assert.Nil(t, e)
		assert.Equal(t, 4, outDF.AsDF().RowCount())

		outDF, e = outDF.AsDF().Parse("where( x>=3.0)")
		assert.Nil(t, e)
		assert.Equal(t, 2, outDF.AsDF().RowCount())

	*/
}

func TestSQLdf_Where(t *testing.T) {
	df, e := df4test()
	assert.Nil(t, e)
	//	e = df2.ParseExpr("ind :=prop_zip3 == '005'", df)
	//	assert.Nil(t, e)
	//	col, ex := df.Parse("l2 := if(prop_zip3=='005', 'yes','no')")
	//	assert.Nil(t, ex)
	//	_ = df.AppendColumn(col.AsColumn(), false)
	col, ex := df.Parse("(latitude/-10.0)*(longitude+exp(1.0))+4.0--3.0")
	assert.Nil(t, ex)
	c := col.AsColumn()
	c.Name("newCol")
	e = df.AppendColumn(c, false)
	assert.Nil(t, e)
	fmt.Println(df.MakeQuery())
	//	col, e = df.Parse("test :='20221231'")
	//	assert.Nil(t, e)
	//	_ = df.AppendColumn(col.AsColumn(), false)
	//	col, e = df.Parse("test1 :=int(4)")
	//	assert.Nil(t, e)
	//	_ = df.AppendColumn(col.AsColumn(), false)
	//	col, e = df.Parse("test2 :=(test != '20221231' || latitude < 41) && test1 >= 3")
	//	assert.Nil(t, e)
	//	_ = df.AppendColumn(col.AsColumn(), false)
	//	col, e = df.Parse("test3 :=test == '20221231' || latitude < 40 && test1 >= 3")
	//	assert.Nil(t, e)
	//	_ = df.AppendColumn(col.AsColumn(), false)
	//	col, e = df.Parse("logical :=prop_zip3 == '005'")
	//	assert.Nil(t, e)
	//	_ = df.AppendColumn(col.AsColumn(), false)

	//	var colx df2.Column
	//	colx, e = df.Column("logical")
	//	assert.Nil(t, e)
	//	newDF, e1 := df.Where(colx)
	//	assert.Nil(t, e1)
	e = df.CreateTable("tmp.aaa", "prop_zip3", true)
	assert.Nil(t, e)
	e = df.DBsave("tmp.aaa", true)
	assert.Nil(t, e)
}

/*
func TestNewSQLdf(t *testing.T) {
	df, e := df4test()
	assert.Nil(t, e)
	defer func() { _ = df.Context.Dialect().DB().Close() }()

	e = df.Apply("testx", "if", false, "1", "latitude", "42.5")
	assert.Nil(t, e)

	//	e = df.Apply("test1", "c", false, "DTint", "1")
	//	assert.Nil(t, e)

	e = df.Apply("test2", "cast", false, "DTstring", "latitude")
	assert.Nil(t, e)

	e2 := df.Apply("c", "add", false, "10", "latitude")
	assert.Nil(t, e2)

	e2 = df.Apply("d", "abs", false, "latitude")
	assert.Nil(t, e2)

	e2 = df.Apply("e", "exp", false, "1.1")
	assert.Nil(t, e2)

	//	e = df.Apply("test1", "cast", "DTstring", "latitude")
	//	assert.Nil(t, e)

	e1 := df.Apply("test", "add", false, "latitude", "longitude")
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


*/
