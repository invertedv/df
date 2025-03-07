package sql

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
	_ "github.com/jackc/pgx/stdlib"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

const (
	pg = "postgres"
	ch = "clickhouse"
)

var which = ch

// NewConnect established a new connection to ClickHouse.
// host is IP address (assumes port 9000), memory is max_memory_usage
func newConnectCH(host, user, password string) (db *sql.DB, err error) {
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

func newConnectPG(host, user, password, dbName string) (db *sql.DB, err error) {
	connectionStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, password, host, dbName)

	if db, err = sql.Open("pgx", connectionStr); err != nil {
		return nil, err
	}

	return db, db.Ping()
}

func testDF(which string) *DF {
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	dbName := os.Getenv("db")

	var (
		db *sql.DB
		e  error
	)

	var tablex string
	if which == pg {
		if db, e = newConnectPG(host, user, password, dbName); e != nil {
			panic(e)
		}
		tablex = "public.d1"
	}

	if which == ch {
		if db, e = newConnectCH(host, user, password); e != nil {
			panic(e)
		}
		tablex = "testing.d1"
	}

	var (
		dialect *d.Dialect
		e1      error
	)
	if dialect, e1 = d.NewDialect(which, db); e1 != nil {
		panic(e1)
	}

	var (
		df *DF
		e2 error
	)
	qry := fmt.Sprintf("SELECT * FROM %s", tablex)
	if df, e2 = DBload(qry, dialect); e2 != nil {
		panic(e2)
	}

	return df
}

func checker(df d.DF, colName string, col d.Column, indx int) any {
	if col != nil {
		// TODO: Wait: should not need this
		_ = df.DropColumns(colName)
		if e := d.ColName(colName)(col); e != nil {
			panic(e)
		}

		if e := df.AppendColumn(col, true); e != nil {
			panic(e)
		}
	}
	q := df.(*DF).MakeQuery()
	memDF, e1 := m.DBLoad(q, df.Dialect())
	if e1 != nil {
		panic(e1)
	}

	if colRet := memDF.Column(colName); colRet != nil {
		if indx < 0 {
			return colRet.Data().AsAny()
		}

		if x := colRet.(*m.Col).Element(indx); x != nil {
			return x
		}
	}

	panic(fmt.Errorf("error in checker"))
}

func TestRowNumber(t *testing.T) {
	dfx := testDF(which)
	out, e := d.Parse(dfx, "rowNumber()")
	q := out.Column().(*Col).MakeQuery()
	fmt.Println(q)
	assert.Nil(t, e)
	_ = d.ColName("rn")(out.Column())
	//	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, checker(dfx, "rn", out.Column(), -1))
	fmt.Println(out.Column().Data().AsAny())
}

func TestNewDFseq(t *testing.T) {
	dfx := testDF(which)
	df, e := NewDFseq(nil, dfx.Dialect(), 5)
	assert.Nil(t, e)
	col := df.Column("seq")
	assert.NotNil(t, col)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, col.Data().AsAny())
}

func TestSQLcol_Data(t *testing.T) {
	const coln = "x"
	dfx := testDF(which)

	c := dfx.Column(coln)
	assert.NotNil(t, c)
	fmt.Println(c.Data().AsAny())
}

func TestWhere(t *testing.T) {
	dfx := testDF(which)
	owner := os.Getenv("user")
	tablespace := os.Getenv("tablespace")

	out, e := d.Parse(dfx, "y == 1 || z == '20060310'")
	assert.Nil(t, e)
	result := checker(dfx, "test", out.Column(), -1)
	assert.Equal(t, []int{1, 0, 0, 1, 0, 1}, result)
	assert.Equal(t, []int{1, 0, 0, 1, 0, 1}, out.Column().Data().AsAny())

	expr := "where(y == 1)"
	out, e = d.Parse(dfx, expr)
	assert.Nil(t, e)
	_ = out

	outDF := out.DF().(*DF)
	fmt.Println(outDF.MakeQuery())
	// save to a table
	var outTable string
	var options []string
	switch which {
	case ch:
		outTable = os.Getenv("chTemp")
	case pg:
		outTable = os.Getenv("pgTemp")
		options = []string{"Owner:" + owner, "TableSpace:" + tablespace}
	}
	e = outDF.Dialect().Save(outTable, "", true, outDF, options...)
	assert.Nil(t, e)
	assert.Equal(t, 2, outDF.RowCount())

}

func TestRename(t *testing.T) {
	dfx := testDF(which)
	e := dfx.Column("x").Rename("yyz")
	//	e := d.ColName("yyz")(c)
	assert.Nil(t, e)
	out, ex := d.Parse(dfx, "1*y")
	assert.Nil(t, ex)
	e = d.ColName("z2")(out.Column())
	assert.Nil(t, e)
	e = out.Column().Rename("zz")
	assert.Nil(t, e)
	q := out.Column().(*Col).MakeQuery()
	_ = q
	d1 := out.Column().Data().AsAny()
	d2 := dfx.Column("y").Data().AsAny()
	assert.Equal(t, d1, d2)
}

func TestCast(t *testing.T) {
	dfx := testDF(which)
	_, e := d.Parse(dfx, "string(23)")
	assert.Nil(t, e)
}

func TestParser(t *testing.T) {
	dfx := testDF(which)

	x := [][]any{
		//		{"x/0.0", 0, math.Inf(1)}, won't work in Postgres
		{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"log(exp(1.0))", 0, 1.0},
		{"((exp(1.0) + log(exp(1.0))))*(3.0--1.0)", 0, 4.0 + 4.0*math.Exp(1)},
		{"float((3.0 * 4.0 + 1.0 - -1.0)*(2.0 + abs(-1.0)))", 0, 42.0},
		{"!(y>=1) && y>=1", 0, 0},
		{"int(abs(-5))", 0, 5},
		{"z=='abc'", 0, 0},
		{"dt != date(20221231)", 0, 0},
		// TODO: fix
		//		{"string(float(1)+.234)", 0, "1.234"},
		{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"3--y", 0, 4},
		{"3--3", 0, 6},
		{"int(-3.0)", 0, -3},
		{"if(y == 1, 2.0, (x))", 0, float64(2)},
		{"if(y == 1, 2.0, (x))", 1, float64(-2)},
		{"4+1--1", 0, int(6)},
		{"exp(x-1.0)", 0, 1.0},
		{"abs(x)", 0, 1.0},
		{"abs(y)", 1, 5},
		{"(x/0.1)*float(y+100)", 0, 1010.0},
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
		{"-x +2.0", 0, float64(1)},
		{"-x +4.0", 1, float64(6)},
		{"(1 + 2) - -(-1 - 2)", 0, 0},
		{"(1.0 + 3.0) / abs(-(-1.0 + 3.0))", 0, float64(2)},
	}

	cnt := 0
	for ind := range len(x) {
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		xOut, ex := d.Parse(dfx, eqn)
		assert.Nil(t, ex)
		fmt.Println(xOut.Column().Data().AsAny())
		result := xOut.Column().Data().Element(x[ind][1].(int))
		if d.WhatAmI(result) == d.DTfloat {
			if result.(float64) == math.Inf(1) && x[ind][2].(float64) == math.Inf(1) {
				continue
			}
			assert.InEpsilon(t, x[ind][2].(float64), result.(float64), .001)
			continue
		}

		if d.WhatAmI(result) == d.DTdate {
			fmt.Println(result.(time.Time).Format("20060102"))
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
	dfx := testDF(which)

	x := [][]any{
		{"sum(y)", 0, 12},
		{"mean(yy)", 0, 32.0 / 6.0},
	}

	cnt := 0
	for ind := range len(x) {
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		xOut, ex := d.Parse(dfx, eqn)
		assert.Nil(t, ex)
		col := xOut.Column()
		_ = d.ColName("test")(col)
		data := col.Data().AsAny()
		fmt.Println(data)

		assert.Equal(t, x[ind][2], col.Data().Element(0))
	}

	fmt.Println("# tests: ", cnt)
}

/*
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
	col := c.Column()
	col.Rename("newCol")
	e = dfx.AppendColumn(col, false)
	assert.Nil(t, e)
	_, e = dfx.AppendDF(dfy)
	assert.NotNil(t, e)
}

func TestMemCol_Replace(t *testing.T) {
	dfx := testDF()
	indCol, e0 := dfx.Parse("y==-5")
	assert.Nil(t, e0)
	indCol.Column().Rename("ind")
	e3 := dfx.AppendColumn(indCol.Column(), false)
	assert.Nil(t, e3)
	coly := dfx.Column("y")
	assert.NotNil(t, coly)
	colyy := dfx.Column("yy")
	assert.NotNil(t, colyy)
	colR, e2 := coly.(*Col).Replace(indCol.Column(), colyy)
	assert.Nil(t, e2)
	assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, checker(dfx, "rep", colR, -1))
}

// TODO: make cat give exact same values btw mem and sql
func TestCat(t *testing.T) {
	dfx := testDF()

	r, e := dfx.Parse("cat(y, 1)")
	assert.Nil(t, e)
	s := r.Column()
	s.Rename("caty")
	//	fmt.Println(s)
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)
	//	col := dfx.Column("caty")
	//	_ = col

	fmt.Println(s)
	r, e = dfx.Parse("int(caty)")
	assert.Nil(t, e)
	r.Column().Rename("test")
	fmt.Println(r.Column())
	//	fmt.Println(dfx)

	//	r, e = dfx.Parse("cat(z)")
	//	assert.Nil(t, e)
	//	s = r.Column()
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
	s := r.Column()
	s.Rename("caty")
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)

	r, e = dfx.Parse("applyCat(yy, caty, -5)")
	assert.Nil(t, e)
	s = r.Column()
	s.Rename("catyy")
	e = dfx.AppendColumn(s, false)
	assert.Nil(t, e)

	e = dfx.Context().Dialect().Save("testing.cat", "", true, dfx)
	assert.Nil(t, e)
}

// mem & sql out of sync
//
*/
