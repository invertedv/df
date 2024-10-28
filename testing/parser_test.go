package testing

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	s "github.com/invertedv/df/sql"

	"github.com/stretchr/testify/assert"
)

const which = "mem"

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

func loadData() d.DF {
	const table = "SELECT * FROM testing.d1"
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

	var dialect *d.Dialect
	dialect, e = d.NewDialect("clickhouse", db)
	ctx := d.NewContext(dialect, nil, nil)

	if which == "sql" {
		df, e1 := s.DBload(table, ctx)
		if e1 != nil {
			panic(e1)
		}
		return df
	}

	df, e2 := m.DBLoad(table, ctx)
	if e2 != nil {
		panic(e2)
	}

	return df
}

func checker(df d.DF, colName string, col d.Column, indx int) any {
	if col != nil {
		col.Name(colName)
		if e := df.AppendColumn(col, true); e != nil {
			panic(e)
		}
	}
	var (
		colRet d.Column
		e      error
	)

	if which == "mem" {
		colRet, e = df.(*m.MemDF).Column(colName)
		if e != nil {
			panic(e)
		}
	}

	if which == "sql" {
		memDF, e1 := m.DBLoad(df.(*s.SQLdf).MakeQuery(), df.Core().Context)
		if e1 != nil {
			panic(e1)
		}
		colRet, _ = memDF.Column(colName)
	}

	if indx < 0 {
		return colRet.Data()
	}

	if x := colRet.(*m.MemCol).Element(indx); x != nil {
		return x
	}

	panic(fmt.Errorf("error in checker"))
}

func TestParse_Table(t *testing.T) {
	dfx := loadData()
	out, e := dfx.Parse("table(y,yy)")
	assert.Nil(t, e)
	df1 := out.AsDF()
	e = df1.Sort(false, "count")
	assert.Nil(t, e)
	assert.Equal(t, []int{2, 1, 1, 1, 1}, checker(df1, "count", nil, -1))
}

func TestParse_Sort(t *testing.T) {
	dfx := loadData()
	_, e := dfx.Parse("sort('asc', y, x)")
	assert.Nil(t, e)
	assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, checker(dfx, "y", nil, -1))
	assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, checker(dfx, "yy", nil, -1))
}

func TestParser(t *testing.T) {
	dfx := loadData()

	x := [][]any{
		{"sum(y)", 0, 12},
		{"sum(x)", 0, 7.5},
		{"dt != date(20221231)", 0, 0},
		{"dt != date(20221231)", 1, 1},
		{"dt == date(20221231)", 0, 1},
		{"dt == date(20221231)", 1, 0},
		{"4+1--1", 0, int(6)},
		{"if(y == 1, 2.0, (x))", 0, float64(2)},
		{"if(y == 1, 2.0, (x))", 1, float64(-2)},
		{"!(y>=1) && y>=1", 0, 0},
		{"exp(x-1.0)", 0, 1.0},
		{"abs(x)", 0, 1.0},
		{"abs(y)", 1, 5},
		{"(x/0.1)*float(y+100)", 0, 1010.0},
		{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"dt != date(20221231)", 0, 0},
		{"dt != date(20221231)", 1, 1},
		{"dt == date(20221231)", 0, 1},
		{"dt == date(20221231)", 1, 0},
		{"string(float(1)+.234)", 0, "1.234"},
		{"float('1.1')", 0, float64(1.1)},
		{"int(2.9)", 0, 2},
		{"float(1)", 0, 1.0},
		{"string(dt)", 0, "2022-12-31"},
		{"z!='20060102'", 0, 1},
		{"x--1.0", 0, 2.0},
		{"x*10.0", 0, 10.0},
		{"int(x)", 5, 3},
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
		{"y == 1", 0, 1},
		{"y == 1", 1, 0},
		{"y && 1", 0, 1},
		{"0 && 1", 0, 0},
		{"0 || 0", 0, 0},
		{"0 || 1", 0, 1},
		{"4+3", 0, int(7)},
		{"4-1-1-1-1", 0, 0},
		{"4+1-1", 0, int(4)},
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
	}

	cnt := 0
	for ind := 0; ind < len(x); ind++ {
		var r d.DF
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		xOut, ex := dfx.Parse(eqn)
		assert.Nil(t, ex)
		xOut.AsColumn().Name("test")
		//m.NewDFcol()
		if which == "sql" {
			r, ex = s.NewDFcol(nil, nil, dfx.(*s.SQLdf).Context, xOut.AsColumn().(*s.SQLcol))
		} else {
			r, ex = m.NewDFcol(nil, nil, dfx.(*m.MemDF).Context, xOut.AsColumn().(*m.MemCol))
		}

		assert.Nil(t, ex)
		result := checker(r, "test", nil, x[ind][1].(int))

		if d.WhatAmI(result) == d.DTfloat {
			assert.InEpsilon(t, x[ind][2].(float64), result.(float64), .001)
			continue
		}

		if d.WhatAmI(result) == d.DTdate {
			assert.Equal(t, result.(time.Time).Year(), x[ind][2].(time.Time).Year())
			assert.Equal(t, result.(time.Time).Month(), x[ind][2].(time.Time).Month())
			assert.Equal(t, result.(time.Time).Day(), x[ind][2].(time.Time).Day())
			continue
		}

		assert.Equal(t, x[ind][2], result)
	}

	fmt.Println("# tests: ", cnt)
}

func TestToCat(t *testing.T) {
	dfx := loadData()
	expr := "date(z)"
	var (
		colx *d.Parsed
		ex   error
	)
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	col.Name("dt1")
	ex = dfx.AppendColumn(col, false)
	assert.Nil(t, ex)

	// try with DTint
	expr = "cat(y)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	colx.AsColumn().Name("test")
	result := checker(dfx, "test", colx.AsColumn(), -1)
	expected := []int{1, 0, 4, 1, 2, 3}
	assert.Equal(t, expected, result)

	// try with DTstring
	expr = "cat(z)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	result = checker(dfx, "test", colx.AsColumn(), -1)
	expected = []int{3, 0, 1, 1, 4, 2}
	assert.Equal(t, expected, result)

	// try with DTdate
	expr = "cat(dt1)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	colx.AsColumn().Name("test")
	result = checker(dfx, "test", colx.AsColumn(), -1)
	expected = []int{3, 0, 1, 1, 4, 2}
	assert.Equal(t, expected, result)

	// try with fuzz > 1
	expr = "cat(y, 2)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	result = checker(dfx, "test", colx.AsColumn(), -1)
	expected = []int{0, -1, -1, 0, -1, -1}
	assert.Equal(t, expected, result)

	// try with DTfloat
	expr = "cat(x)"
	colx, ex = dfx.Parse(expr)
	assert.NotNil(t, ex)
}

func TestApplyCat(t *testing.T) {
	dfx := loadData()
	r, e := dfx.Parse("cat(y)")
	assert.Nil(t, e)
	s := r.AsColumn()
	s.Name("caty")
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)

	r, e = dfx.Parse("applyCat(yy, caty, -5)")
	assert.Nil(t, e)
	s = r.AsColumn()
	s.Name("test")
	result := checker(dfx, "test", s, -1)

	// -5 maps to 0 so all new values map to 0
	expected := []int{1, 0, 0, 1, 0, 0}
	assert.Equal(t, expected, result)

	// try with fuzz > 1
	r, e = dfx.Parse("cat(y,2)")
	assert.Nil(t, e)
	r.AsColumn().Name("caty2")
	e = dfx.AppendColumn(r.AsColumn(), false)
	assert.Nil(t, e)

	r, e = dfx.Parse("applyCat(yy,caty2,-5)")
	assert.Nil(t, e)
	r.AsColumn().Name("test")
	result = checker(dfx, "test", r.AsColumn(), -1)
	expected = []int{0, -1, -1, 0, -1, -1}
	assert.Equal(t, expected, result)
}

func TestAppendDF(t *testing.T) {
	dfx := loadData()
	dfy := loadData()
	dfOut, e := dfx.AppendDF(dfy)
	assert.Nil(t, e)
	exp := dfx.RowCount() + dfy.RowCount()
	assert.Equal(t, exp, dfOut.RowCount())
}
