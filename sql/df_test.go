package sql

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	df2 "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"

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

func df4test() *SQLdf {
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")

	var (
		db *sql.DB
		e  error
	)

	if db, e = newConnect(host, user, password); e != nil {
		panic(e)
	}

	var dialect *df2.Dialect
	dialect, e = df2.NewDialect("clickhouse", db)

	// , ln_zb_dt
	//	df, e1 := NewSQLdf("SELECT ln_id, vintage, ln_orig_ir, last_dt FROM fannie.final limit 10000", df2.NewContext(dialect, nil, nil))
	df, e1 := NewSQLdf("SELECT * FROM testing.d1", df2.NewContext(dialect, nil, nil))
	if e1 != nil {
		panic(e)
	}

	return df
}

func checker(df df2.DF, tableName, colName string) *m.MemCol {
	e := df.CreateTable(tableName, "y", true)
	if e != nil {
		panic(e)
	}
	e = df.DBsave(tableName, true)
	if e != nil {
		panic(e)
	}
	memDF, e1 := m.DBLoad(fmt.Sprintf("SELECT * FROM %s", tableName), df.(*SQLdf).Dialect())
	if e1 != nil {
		panic(e1)
	}
	colm, e2 := memDF.Column(colName)
	if e2 != nil {
		panic(e2)
	}
	return colm.(*m.MemCol)

}

func TestWhere(t *testing.T) {
	dfx := df4test()
	defer func() { _ = dfx.Context.Dialect().DB().Close() }()

	out, e := dfx.Parse("y == 1 || z == '20060310'")
	assert.Nil(t, e)
	col := out.AsColumn().(*SQLcol)
	col.Name("test")
	e = dfx.AppendColumn(col, false)
	assert.Nil(t, e)
	assert.Equal(t, []int{0, 1, 1, 0, 0, 1}, checker(dfx, "testing.bbb", "test").Data())

	expr := "where(y == 1)"
	out, e = dfx.Parse(expr)
	assert.Nil(t, e)
	outDF := out.AsDF().(*SQLdf)
	//assert.Equal(t, 10, outDF.AsDF().RowCount())
	fmt.Println(outDF.MakeQuery())
	e = outDF.CreateTable("testing.aaa", "y", true)
	assert.Nil(t, e)
	e = outDF.DBsave("testing.aaa", true)
	assert.Nil(t, e)
	assert.Equal(t, 2, outDF.RowCount())
}

func TestParser(t *testing.T) {
	dfx := df4test()

	x := [][]any{
		{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"(x/0.1)*float(y+100)", 0, 1010.0},
		{"dt != date(20221231)", 0, 0},
		{"dt != date(20221231)", 1, 1},
		{"dt == date(20221231)", 0, 1},
		{"dt == date(20221231)", 1, 0},
		{"float(1)", 0, 1.0},
		{"z!='20060102'", 0, 1},
		{"x--1.0", 0, 2.0},
		{"x*10.0", 0, 10.0},
		{"int(x)", 5, 3},
		{"sum(y)", 0, 12},
		{"mean(yy)", 0, float64(32) / 6.0},
		{"if(y == 1, 2.0, (x))", 0, float64(1)},
		{"if(y == 1, 2.0, (x))", 1, float64(2)},
		{"(float(4+2) * abs(-3.0/2.0))", 0, float64(9)},
		{"y != 1", 0, 0},
		{"y>=1 && y>=1 && dt >= date(20221231)", 0, 1},
		{"y>=1 && y>=1 && dt > date(20221231)", 0, 0},
		{"y>=1 && y>=1", 0, 1},
		{"!(y>=1) && y>=1", 0, 0},
		{"!1 && 1 || 1", 0, 1},
		{"!1 && 1 || 0", 0, 0},
		{"!0 && 1 || 0", 0, 1},
		{"!1 && 1", 0, 0},
		{"1 || 0 && 1", 0, 1},
		{"0 || 0 && 1", 0, 0},
		{"0 || 1 && 1", 0, 1},
		{"0 || 1 && 1 && 0", 0, 0},
		{"(0 || 1 && 1) && 0", 0, 0},
		{"y < 2", 0, 1},
		{"y < 1", 0, 0},
		{"y <= 1", 0, 1},
		{"y > 1", 0, 0},
		{"y >= 1", 0, 1},
		{"dt != date(20221231)", 0, 0},
		{"dt != date(20221231)", 1, 1},
		{"dt == date(20221231)", 0, 1},
		{"dt == date(20221231)", 1, 0},
		{"y == 1", 0, 1},
		{"y == 1", 1, 0},
		{"y && 1", 0, 1},
		{"0 && 1", 0, 0},
		{"0 || 0", 0, 0},
		{"0 || 1", 0, 1},
		{"4+3", 0, int(7)},
		{"4-1-1-1-1", 0, 0},
		{"4+1-1", 0, int(4)},
		{"4+1--1", 0, int(6)},
		{"float(4)+1.0--1.0", 0, float64(6)},
		{"exp(1.0)*abs(float(-2/(1+1)))", 0, math.Exp(1)},
		{"date( 20020630)", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
		{"date('2002-06-30')", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
		{"((exp(1.0) + log(exp(1.0))))*(3.0--1.0)", 0, 4.0 + 4.0*math.Exp(1)},
		{"-x +2.0", 0, float64(1)},
		{"-x +4.0", 1, float64(6)},
		{"x/0.0", 0, math.Inf(1)},
		{"(3.0 * 4.0 + 1.0 - -1.0)*(2.0 + abs(-1.0))", 0, float64(42)},
		{"(1 + 2) - -(-1 - 2)", 0, 0},
		{"(1.0 + 3.0) / abs(-(-1.0 + 3.0))", 0, float64(2)},
		{"string(float(1))", 0, "1.00"},
		{"float('1.1')", 0, float64(1.1)},
		{"int(2.9)", 0, 2},
	}

	cnt := 0
	for ind := 0; ind < len(x); ind++ {
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		xOut, ex := dfx.Parse(eqn)
		assert.Nil(t, ex)
		col := xOut.AsColumn()
		col.Name("test")
		e := dfx.AppendColumn(col, true)
		assert.Nil(t, e)

		indx := x[ind][1].(int)

		result := checker(dfx, "testing.check", "test").Element(indx)

		if df2.WhatAmI(result) == df2.DTfloat {
			assert.InEpsilon(t, result.(float64), x[ind][2].(float64), .001)
			continue
		}

		if df2.WhatAmI(result) == df2.DTdate {
			assert.Equal(t, result.(time.Time).Year(), x[ind][2].(time.Time).Year())
			assert.Equal(t, result.(time.Time).Month(), x[ind][2].(time.Time).Month())
			assert.Equal(t, result.(time.Time).Day(), x[ind][2].(time.Time).Day())
			continue
		}

		assert.Equal(t, result, x[ind][2])
	}

	fmt.Println("# tests: ", cnt)
}
