package sql

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	d "github.com/invertedv/df"
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

func testDF() *SQLdf {
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

	var (
		dialect *d.Dialect
		e1      error
	)
	if dialect, e1 = d.NewDialect("clickhouse", db); e1 != nil {
		panic(e1)
	}

	var (
		df *SQLdf
		e2 error
	)
	if df, e2 = DBload("SELECT * FROM testing.d1", d.NewContext(dialect, nil, nil)); e2 != nil {
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
	q := df.(*SQLdf).MakeQuery()
	memDF, e1 := m.DBLoad(q, df.(*SQLdf).Context().Dialect())
	if e1 != nil {
		panic(e1)
	}

	if colRet, e := memDF.Column(colName); e == nil {
		if indx < 0 {
			return colRet.Data()
		}

		if x := colRet.(*m.MemCol).Element(indx); x != nil {
			return x
		}
	}

	panic(fmt.Errorf("error in checker"))
}

func TestSQLcol_Data(t *testing.T) {
	const coln = "x"
	dfx := testDF()
	c, e := dfx.Column(coln)
	assert.Nil(t, e)
	fmt.Println(c.Data())
}

func TestWhere(t *testing.T) {
	dfx := testDF()
	defer func() { _ = dfx.Context().Dialect().DB().Close() }()

	out, e := dfx.Parse("y == 1 || z == '20060310'")
	assert.Nil(t, e)
	result := checker(dfx, "test", out.AsColumn(), -1)
	assert.Equal(t, []int{1, 0, 0, 1, 0, 1}, result)

	expr := "where(y == 1)"
	out, e = dfx.Parse(expr)
	assert.Nil(t, e)
	outDF := out.AsDF().(*SQLdf)
	fmt.Println(outDF.MakeQuery())
	e = outDF.Context().Dialect().Save("testing.where", "", true, outDF)
	assert.Nil(t, e)
	assert.Equal(t, 2, outDF.RowCount())
}

func TestParser(t *testing.T) {
	dfx := testDF()

	x := [][]any{
		{"string(float(1)+.234)", 0, "1.234"},
		{"dt != date(20221231)", 0, 0},
		{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"3--y", 0, 4},
		{"3--3", 0, 6},
		{"int(-3.0)", 0, -3},
		{"if(y == 1, 2.0, (x))", 0, float64(2)},
		{"if(y == 1, 2.0, (x))", 1, float64(-2)},
		{"4+1--1", 0, int(6)},
		{"!(y>=1) && y>=1", 0, 0},
		{"exp(x-1.0)", 0, 1.0},
		{"abs(x)", 0, 1.0},
		{"abs(y)", 1, 5},
		{"(x/0.1)*float(y+100)", 0, 1010.0},
		{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"dt != date(20221231)", 1, 1},
		{"dt == date(20221231)", 0, 1},
		{"dt == date(20221231)", 1, 0},
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
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		xOut, ex := dfx.Parse(eqn)
		assert.Nil(t, ex)
		result := checker(dfx, "test", xOut.AsColumn(), x[ind][1].(int))

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

func TestParserS(t *testing.T) {
	dfx := testDF()

	x := [][]any{
		{"sum(y)", 0, 12},
		{"mean(yy)", 0, float64(32) / 6.0},
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
		var (
			dfNew *SQLdf
			e     error
		)
		ez := dfx.AppendColumn(col, true)
		assert.NotNil(t, ez)

		dfNew, e = NewDFcol(nil, nil, dfx.Context(), col.(*SQLcol))
		assert.Nil(t, e)
		indx := x[ind][1].(int)

		result := checker(dfNew, "test", nil, indx)

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

func TestSQLdf_Version(t *testing.T) {
	dfx := testDF()
	dfOld := dfx.Copy()
	result, e := dfx.Parse("2.0*x")
	assert.Nil(t, e)
	col := result.AsColumn()
	col.Name("x2")
	e = dfx.AppendColumn(col, false)
	assert.Nil(t, e)
	assert.Equal(t, 1, dfx.Version())

	result, e = dfx.Parse("abs(x2)")
	assert.Nil(t, e)
	col = result.AsColumn()
	col.Name("absx2")

	result, e = dfx.Parse("2*y")
	assert.Nil(t, e)
	col1 := result.AsColumn()
	col1.Name("y2")

	// add absx2 to an older version of dfx -- this is not OK
	e = dfOld.AppendColumn(col, false)
	assert.NotNil(t, e)

	// add absx2 to current version of dfx -- this is OK
	e = dfx.AppendColumn(col, false)
	assert.Nil(t, e)

	data := checker(dfx, "absx2", col, -1)
	assert.Equal(t, data, []float64{2, 4, 6, 0, 4, 7})
	fmt.Println(data)

	assert.Equal(t, dfx.Version(), 3)
	assert.Equal(t, col1.(*SQLcol).Version(), 1)

	// add col1 to a newer version of dfx -- this is OK
	e = dfx.AppendColumn(col1, false)
	assert.Nil(t, e)
}

// TODO: implement SORT
func TestSQLdf_Table(t *testing.T) {
	dfx := testDF()
	dfTable, e := dfx.Parse("table(y,yy)")
	assert.Nil(t, e)
	e = dfTable.AsDF().Sort(true, "count")
	assert.Nil(t, e)
	outCol := checker(dfTable.AsDF(), "count", nil, -1)
	assert.Equal(t, []int{1, 1, 1, 1, 2}, outCol)
}

func TestSQLcol_String(t *testing.T) {
	dfx := testDF()
	fmt.Println(dfx)
	/*	cx, _ := dfx.Column("x")
		fmt.Println(cx)

		cx, _ = dfx.Column("y")
		fmt.Println(cx)

		cx, _ = dfx.Column("dt")
		fmt.Println(cx)

	*/
}

func TestSQLdf_AppendDF(t *testing.T) {
	dfx := testDF()
	dfy := testDF()
	dfOut, e := dfx.AppendDF(dfy)
	assert.Nil(t, e)
	e = dfx.Context().Dialect().Save("testing.append", "k", true, dfOut)
	assert.Nil(t, e)
	var c *d.Parsed
	c, e = dfx.Parse("exp(x)")
	assert.Nil(t, e)
	col := c.AsColumn()
	col.Name("newCol")
	e = dfx.AppendColumn(col, false)
	assert.Nil(t, e)
	_, e = dfx.AppendDF(dfy)
	assert.NotNil(t, e)
}

func TestMemCol_Replace(t *testing.T) {
	dfx := testDF()
	indCol, e0 := dfx.Parse("y==-5")
	assert.Nil(t, e0)
	indCol.AsColumn().Name("ind")
	e3 := dfx.AppendColumn(indCol.AsColumn(), false)
	assert.Nil(t, e3)
	coly, e := dfx.Column("y")
	assert.Nil(t, e)
	colyy, e1 := dfx.Column("yy")
	assert.Nil(t, e1)
	colR, e2 := coly.(*SQLcol).Replace(indCol.AsColumn(), colyy)
	assert.Nil(t, e2)
	assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, checker(dfx, "rep", colR, -1))
}

// TODO: make cat give exact same values btw mem and sql
func TestCat(t *testing.T) {
	dfx := testDF()

	r, e := dfx.Parse("cat(y, 1)")
	assert.Nil(t, e)
	s := r.AsColumn()
	s.Name("caty")
	fmt.Println(s)
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)
	//	fmt.Println(s)
	r, e = dfx.Parse("int(caty)")
	assert.Nil(t, e)
	r.AsColumn().Name("test")
	fmt.Println(r.AsColumn())
	//	fmt.Println(dfx)

	//	r, e = dfx.Parse("cat(z)")
	//	assert.Nil(t, e)
	//	s = r.AsColumn()
	//	s.Name("catz")
	//	e = dfx.AppendColumn(s, false)
	//	assert.Nil(t, e)

	//e = dfx.DBsave("testing.cat", true)
	e = dfx.Context().Dialect().Save("testing.cat", "", true, dfx)
	assert.Nil(t, e)
}

func TestParse_Sort(t *testing.T) {
	dfx := testDF()
	_, e := dfx.Parse("sort('asc', y, x)")
	assert.Nil(t, e)
	assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, checker(dfx, "y", nil, -1))
	assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, checker(dfx, "yy", nil, -1))

	_, e = dfx.Parse("sort('desc', y, x)")
	assert.Nil(t, e)
	assert.Equal(t, []int{6, 5, 4, 1, 1, -5}, checker(dfx, "y", nil, -1))
	assert.Equal(t, []int{16, 14, 15, 1, 1, -15}, checker(dfx, "yy", nil, -1))
}

func TestApplyCat(t *testing.T) {
	dfx := testDF()
	r, e := dfx.Parse("cat(y)")
	assert.Nil(t, e)
	s := r.AsColumn()
	s.Name("caty")
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)

	r, e = dfx.Parse("applyCat(yy, caty, int(abs(-5.0)))")
	assert.Nil(t, e)
	s = r.AsColumn()
	s.Name("catyy")
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)

	e = dfx.Context().Dialect().Save("testing.cat", "", true, dfx)
	assert.Nil(t, e)
}

// mem & sql out of sync
//
