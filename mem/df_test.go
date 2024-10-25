package df

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	d "github.com/invertedv/df"
	"github.com/stretchr/testify/assert"
)

func testDF() *MemDF {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, 3.5})
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	yy, _ := NewMemCol("yy", []int{1, -15, 16, 1, 15, 14})
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310"})
	dfx, e := NewDFcol(RunDFfn, StandardFunctions(), nil, x, y, z, yy)
	if e != nil {
		panic(e)
	}

	xx, _ := NewMemCol("r", []int{1, 2, 3, 1, 2, 3})
	if e := dfx.AppendColumn(xx, false); e != nil {
		panic(e)
	}

	return dfx
}

func checker(df d.DF, colName string, col d.Column, indx int) any {
	if col != nil {
		col.Name(colName)
		if e := df.AppendColumn(col, true); e != nil {
			panic(e)
		}
	}

	if colRet, e := df.Column(colName); e == nil {
		if indx < 0 {
			return colRet.Data()
		}

		if x := colRet.(*MemCol).Element(indx); x != nil {
			return x
		}
	}

	panic(fmt.Errorf("error in checker"))
}

func TestParse_Sort(t *testing.T) {
	dfx := testDF()
	_, e := dfx.Parse("sort('asc', y, x)")
	assert.Nil(t, e)
	assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, checker(dfx, "y", nil, -1))
	assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, checker(dfx, "yy", nil, -1))
}

func TestParse_Table(t *testing.T) {
	dfx := testDF()
	df1, e := dfx.Parse("table(y,yy)")
	assert.Nil(t, e)
	fmt.Println(df1.AsDF().Column("count"))
	col, _ := df1.AsDF().Column("count")
	assert.Equal(t, []int{2, 1, 1, 1, 1}, col.Data())
	e = df1.AsDF().Sort(true, "y", "yy")
	assert.Nil(t, e)
	fmt.Println(df1.AsDF().Column("y"))
}

func TestParser(t *testing.T) {
	dfx := testDF()
	eqn := "date(z)"
	colx, e := d.ParseExpr(eqn, dfx.DFcore)
	assert.Nil(t, e)
	col := colx.AsColumn()
	col.Name("dt")
	e = dfx.AppendColumn(col, true)
	assert.Nil(t, e)

	x := [][]any{
		{"4+1--1", 0, int(6)},
		{"if(y == 1, 2.0, (x))", 0, float64(2)},
		{"if(y == 1, 2.0, (x))", 1, float64(-2)},
		{"string(dt)", 0, "2022-12-31"},
		{"string(float(1)+.234)", 0, "1.234"},
		{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"z!='20060102'", 0, 1},
		{"x--1.0", 0, 2.0},
		{"(x/0.1)*float(y+100)", 0, 1010.0},
		{"x*10.0", 0, 10.0},
		{"int(x)", 5, 3},
		{"sum(y)", 0, 12},
		{"mean(yy)", 0, float64(32) / 6.0},
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

		result := checker(dfx, "test", xOut.AsColumn(), x[ind][1].(int))

		if xOut.AsColumn().DataType() == d.DTfloat {
			assert.InEpsilon(t, result.(float64), x[ind][2].(float64), .001)
			continue
		}

		assert.Equal(t, result, x[ind][2])
	}

	fmt.Println("# tests: ", cnt)
}

func TestFuzzCat(t *testing.T) {
	dfx := testDF()
	expr := "cat(y)"

	colx, ex := dfx.Parse(expr)
	col := colx.AsColumn()
	col.Name("c")
	assert.Nil(t, ex)
	_ = dfx.AppendColumn(col, false)

	expr = "fuzzCat(c, 2, 100)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	exp := []int{0, -1, -1, 0, -1, -1}
	assert.Equal(t, exp, colx.AsColumn().Data())
}

func TestApplyCat(t *testing.T) {
	//	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	//	yy, _ := NewMemCol("yy", []int{1, -15, 16, 1, 4, 5})
	dfx := testDF()
	expr := "cat(y)"
	colx, ex := dfx.Parse(expr)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	col.Name("c")
	_ = dfx.AppendColumn(col, false)

	v := col.(*MemCol).catMap[6]

	// default is a known category level
	expr = "applyCat(yy, c, 6)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	exp := []int{0, v, v, 0, v, v}
	assert.Equal(t, exp, colx.AsColumn().Data())

	// default is not a known category level
	expr = "applyCat(yy, c, 100)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	v = -1
	exp = []int{0, v, v, 0, v, v}
	assert.Equal(t, exp, colx.AsColumn().Data())

	expr = "c + y"
	_, ex = dfx.Parse(expr)
	assert.NotNil(t, ex)
}

func TestToCat(t *testing.T) {
	//	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	// z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310"})
	dfx := testDF()
	expr := "date(z)"
	var (
		colx *d.Parsed
		ex   error
	)
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	col.Name("dt")
	ex = dfx.AppendColumn(col, false)
	assert.Nil(t, ex)

	expr = "cat(y)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)

	result := checker(dfx, "test", colx.AsColumn(), -1)
	expected := []int{0, 1, 2, 0, 3, 4}
	assert.Equal(t, expected, result)

	expr = "cat(z)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	result = checker(dfx, "test", colx.AsColumn(), -1)
	expected = []int{0, 1, 2, 2, 3, 4}
	assert.Equal(t, expected, result)

	expr = "cat(dt)"
	colx, ex = dfx.Parse(expr)
	assert.Nil(t, ex)
	result = checker(dfx, "test", colx.AsColumn(), -1)
	expected = []int{0, 1, 2, 2, 3, 4}
	assert.Equal(t, expected, result)

	expr = "cat(x)"
	colx, ex = dfx.Parse(expr)
	assert.NotNil(t, ex)
}

func TestSumx(t *testing.T) {
	dfx := testDF()
	var (
		col *d.Parsed
		e   error
	)
	col, e = dfx.Parse("float(sum(y))*x")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("sum(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("-x")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("x > 0.0 || y == 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("log(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("exp(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("string(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("date(z)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("int(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("float(y)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("if(x>1.0,y,yy)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("x/2.0")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("2-y")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("2+y")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("x >= 1.0 && y==1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("abs(y)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y == 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y >= 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y > 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y <= 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y < 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = dfx.Parse("y != 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

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

	var dialect *d.Dialect
	dialect, e = d.NewDialect("clickhouse", db)
	ctx := d.NewContext(dialect, nil, nil)
	assert.Nil(t, e)
	memDF, e1 := DBLoad("SELECT * FROM zip.zip3 LIMIT 10", ctx)
	assert.Nil(t, e1)
	col, e2 := memDF.Column("prop_zip3")
	assert.Nil(t, e2)
	fmt.Println(col.Data())

	ed := memDF.CreateTable("tmp.aaa", "prop_zip3", true, "prop_zip3", "latitude")
	assert.Nil(t, ed)
	fmt.Println("len", memDF.Len())
	ed = memDF.FileSave("/home/will/tmp/test.csv")
	assert.Nil(t, ed)
}

func TestWhere(t *testing.T) {
	dfx := testDF()
	expr := "where(y>1)"
	outDF, e := dfx.Parse(expr)
	assert.Nil(t, e)
	assert.Equal(t, 3, outDF.AsDF().RowCount())

	outDF, e = dfx.Parse("where(x >= 1.0)")
	assert.Nil(t, e)
	assert.Equal(t, 4, outDF.AsDF().RowCount())

	outDF, e = outDF.AsDF().Parse("where( x>=3.0)")
	assert.Nil(t, e)
	assert.Equal(t, 2, outDF.AsDF().RowCount())
}

func TestAppendRows(t *testing.T) {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, 3})
	y, _ := NewMemCol("x", []float64{1, 2, 3})

	z, e := AppendRows(x, y, "test")
	assert.Nil(t, e)
	assert.Equal(t, float64(-2), z.Element(1))
	assert.Equal(t, float64(3), z.Element(8))

	x, _ = NewMemCol("x", []string{"a", "b", "c"})
	y, _ = NewMemCol("x", []string{"d", "e", "f"})

	z, e = AppendRows(x, y, "test")
	assert.Nil(t, e)
	assert.Equal(t, "b", z.Element(1))
	assert.Equal(t, "e", z.Element(4))
}

func TestMemDF_AppendDF(t *testing.T) {
	dfx := testDF()
	dfy := testDF()

	dfz, e := dfx.AppendDF(dfy)
	assert.Nil(t, e)
	var col d.Column
	col, e = dfz.Column("x")
	assert.Nil(t, e)
	assert.Equal(t, float64(1), col.(*MemCol).Element(0))
	assert.Equal(t, float64(1), col.(*MemCol).Element(dfx.RowCount()))
}

func TestMemDF_Table(t *testing.T) {
	x, _ := NewMemCol("x", []int{1, -5, 6, 1, 4, 5, 4, 4}) //5:  0, 1, 2, 0, 3, 4, 3, 3
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 3, 5, 4, 4}) //6:  0, 1, 2, 0, 3, 4, 5, 5
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310", "20160430", "20160430"})
	dfx, e := NewDFcol(RunDFfn, StandardFunctions(), nil, x, y, z)
	assert.Nil(t, e)
	dtx, ex := dfx.Parse("date(z)")
	assert.Nil(t, ex)
	dt := dtx.AsColumn()
	dt.Name("dt")
	e = dfx.AppendColumn(dt, false)
	assert.Nil(t, e)

	var tab d.DF
	tab, e = dfx.Table(false, "x", "y")
	assert.Nil(t, e)
	cNames := tab.ColumnNames()
	for ind := 0; ind < len(cNames); ind++ {
		col, _ := tab.Column(cNames[ind])
		fmt.Println(cNames[ind])
		fmt.Println(col.Data())
	}
}
