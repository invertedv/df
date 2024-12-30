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

func testDF() *DF {
	x, _ := NewCol([]float64{1, -2, 3, 0, 2, 3.5}, d.DTfloat, d.ColName("x"))
	y, _ := NewCol([]int{1, -5, 6, 1, 4, 5}, d.DTint, d.ColName("y"))
	yy, _ := NewCol([]int{1, -15, 16, 1, 15, 14}, d.DTint, d.ColName("yy"))
	z, _ := NewCol([]string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310"}, d.DTstring, d.ColName("z"))
	dfx, e := NewDFcol(StandardFunctions(), []*Col{x, y, z, yy})
	if e != nil {
		panic(e)
	}

	xx, _ := NewCol([]int{1, 2, 3, 1, 2, 3}, d.DTint, d.ColName("r"))
	if e := dfx.AppendColumn(xx, false); e != nil {
		panic(e)
	}

	return dfx
}

func checker(df d.DF, colName string, col d.Column, indx int) any {
	if col != nil {
		d.ColName(colName)(col)
		if e := df.AppendColumn(col, true); e != nil {
			panic(e)
		}
	}

	if colRet := df.Column(colName); colRet != nil {
		if indx < 0 {
			return colRet.Data()
		}

		if x := colRet.(*Col).Element(indx); x != nil {
			return x
		}
	}

	panic(fmt.Errorf("error in checker"))
}

func TestRowNumber(t *testing.T) {
	dfx := testDF()
	out, e := d.Parse(dfx, "rowNumber()")
	assert.Nil(t, e)

	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, out.AsColumn().Data().AsInt())
}

func TestParse_Sort(t *testing.T) {
	dfx := testDF()
	_, e := d.Parse(dfx, "sort('asc', y, x)")
	assert.Nil(t, e)
	assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, dfx.Column("y").Data().AsInt())
	assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, dfx.Column("yy").Data().AsInt())
}

func TestParse_Table(t *testing.T) {
	dfx := testDF()
	df1, e := d.Parse(dfx, "table(y,yy)")
	assert.Nil(t, e)
	fmt.Println(df1.AsDF().Column("count"))
	col := df1.AsDF().Column("count")
	assert.Equal(t, []int{2, 1, 1, 1, 1}, col.Data().AsInt())
	e = df1.AsDF().Sort(true, "y", "yy")
	assert.Nil(t, e)
	fmt.Println(df1.AsDF().Column("y"))
}

func TestParser(t *testing.T) {
	dfx := testDF()
	eqn := "date(z)"
	colx, e := d.ParseExpr(eqn, dfx)
	assert.Nil(t, e)
	col := colx.AsColumn()
	d.ColName("dt")(col)
	e = dfx.AppendColumn(col, true)
	assert.Nil(t, e)

	x := [][]any{
		{"string(float(1)+.234)", 0, "1.234"},
		{"4+1--1", 0, int(6)},
		{"if(y == 1, 2.0, (x))", 0, float64(2)},
		{"if(y == 1, x, 2.0)", 1, float64(2)},
		{"string(dt)", 0, "2022-12-31"},
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
		xOut, ex := d.Parse(dfx, eqn)
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

func TestNewDFseq(t *testing.T) {
	df := NewDFseq(nil, 5)
	col := df.Column("seq")
	assert.NotNil(t, col)
	assert.Equal(t, []int{0, 1, 2, 3, 4}, col.Data().AsInt())
}

func TestApplyCat(t *testing.T) {
	//	y, _ := NewCol("y", []int{1, -5, 6, 1, 4, 5})
	//	yy, _ := NewCol("yy", []int{1, -15, 16, 1, 4, 5})
	dfx := testDF()
	expr := "cat(y)"
	colx, ex := d.Parse(dfx, expr)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	d.ColName("c")(col)
	_ = dfx.AppendColumn(col, false)
	fmt.Println(col)
	back, ex := d.Parse(dfx, "int(c)")
	d.ColName("test")(back.AsColumn())
	assert.Nil(t, ex)
	fmt.Println(back.AsColumn())

	v := col.(*Col).CategoryMap()[6]

	// default is a known category level
	expr = "applyCat(yy, c, 6)"
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)
	exp := []int{1, v, v, 1, v, v}
	assert.Equal(t, exp, colx.AsColumn().Data().AsInt())

	// default is not a known category level
	expr = "applyCat(yy, c, 100)"
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)
	v = -1
	exp = []int{1, v, v, 1, v, v}
	assert.Equal(t, exp, colx.AsColumn().Data().AsInt())

	expr = "c + y"
	_, ex = d.Parse(dfx, expr)
	assert.NotNil(t, ex)
}

func TestToCat(t *testing.T) {
	//	y, _ := NewCol("y", []int{1, -5, 6, 1, 4, 5}) -5, 1, 1, 4, 5, 6
	// z, _ := NewCol("z", []string{"20221231"/3, "20000101"/0, "20060102"/1, "20060102"/1, "20230915"/4, "20060310"/2})
	dfx := testDF()
	expr := "date(z)"
	var (
		colx *d.Parsed
		ex   error
	)
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	d.ColName("dt")(col)
	ex = dfx.AppendColumn(col, false)
	assert.Nil(t, ex)

	expr = "cat(y)"
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)

	expected := []int{1, 0, 4, 1, 2, 3}
	assert.Equal(t, expected, colx.AsColumn().Data().AsInt())

	expr = "cat(z)"
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)
	expected = []int{3, 0, 1, 1, 4, 2}
	assert.Equal(t, expected, colx.AsColumn().Data().AsInt())

	expr = "cat(dt)"
	colx, ex = d.Parse(dfx, expr)
	assert.Nil(t, ex)
	expected = []int{3, 0, 1, 1, 4, 2}
	assert.Equal(t, expected, colx.AsColumn().Data().AsInt())

	expr = "cat(x)"
	colx, ex = d.Parse(dfx, expr)
	assert.NotNil(t, ex)
}

func TestSumx(t *testing.T) {
	dfx := testDF()
	var (
		col *d.Parsed
		e   error
	)
	col, e = d.Parse(dfx, "float(sum(y))*x")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "sum(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "-x")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "x > 0.0 || y == 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "log(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "exp(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "string(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "date(z)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "int(x)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "float(y)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "if(x>1.0,y,yy)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "x/2.0")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "2-y")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "2+y")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "x >= 1.0 && y==1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "abs(y)")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y == 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y >= 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y > 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y <= 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y < 1")
	assert.Nil(t, e)
	fmt.Println(col.AsColumn().Data())

	col, e = d.Parse(dfx, "y != 1")
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
	assert.Nil(t, e)
	memDF, e1 := DBLoad("SELECT * FROM zip.zip3 LIMIT 10", dialect)
	assert.Nil(t, e1)
	col := memDF.Column("prop_zip3")
	assert.NotNil(t, col)
	fmt.Println(col.Data())

	//	ed := memDF.CreateTable("tmp.aaa", "prop_zip3", true, "prop_zip3", "latitude")
	//	assert.Nil(t, ed)
	fmt.Println("len", memDF.Len())
	f := d.NewFiles()
	ed := f.Save("/home/will/tmp/test.csv", memDF)
	assert.Nil(t, ed)
}

func TestMemCol_String(t *testing.T) {
	dfx := testDF()
	fmt.Println(dfx)
}

func TestMemCol_Replace(t *testing.T) {
	dfx := testDF()
	//	indCol, e0 := d.Parse(dfx,"y==-5")
	//	assert.Nil(t, e0)
	coly := dfx.Column("y")
	assert.NotNil(t, coly)
	colyy := dfx.Column("yy")
	assert.NotNil(t, colyy)
	//	colR, e2 := coly.(*Col).Replace(indCol.AsColumn(), colyy)
	//	assert.Nil(t, e2)
	//	assert.Equal(t, colR.(*Col).Data(), []int{1, -15, 6, 1, 4, 5})
}

func TestVector(t *testing.T) {
	v, _ := d.NewVector([]int{1, 2, 3, 4}, d.DTint)
	vx, _ := d.NewVector(v.AsString(), d.DTstring) // v.Coerce(d.DTstring)
	assert.NotNil(t, vx)
}

func TestWhere(t *testing.T) {
	dfx := testDF()
	expr := "where(y>1)"
	outDF, e := d.Parse(dfx, expr)
	assert.Nil(t, e)
	assert.Equal(t, 3, outDF.AsDF().RowCount())

	outDF, e = d.Parse(dfx, "where(x >= 1.0)")
	assert.Nil(t, e)
	assert.Equal(t, 4, outDF.AsDF().RowCount())

	outDF, e = d.Parse(outDF.AsDF(), "where( x>=3.0)")
	assert.Nil(t, e)
	assert.Equal(t, 2, outDF.AsDF().RowCount())
}

func TestAppendRows(t *testing.T) {
	x, _ := NewCol([]float64{1, -2, 3, 0, 2, 3}, d.DTfloat, d.ColName("x"))
	y, _ := NewCol([]float64{1, 2, 3}, d.DTfloat, d.ColName("x"))

	z, e := appendRows(x, y)
	assert.Nil(t, e)
	assert.Equal(t, float64(-2), z.Element(1))
	assert.Equal(t, float64(3), z.Element(8))

	x, _ = NewCol([]string{"a", "b", "c"}, d.DTstring, d.ColName("x"))
	y, _ = NewCol([]string{"d", "e", "f"}, d.DTstring, d.ColName("x"))

	z, e = appendRows(x, y)
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
	col = dfz.Column("x")
	assert.NotNil(t, col)
	assert.Equal(t, float64(1), col.(*Col).Element(0))
	assert.Equal(t, float64(1), col.(*Col).Element(dfx.RowCount()))
}

func TestMemDF_Table(t *testing.T) {
	x, _ := NewCol([]int{1, -5, 6, 1, 4, 5, 4, 4}, d.DTint, d.ColName("x")) //5:  0, 1, 2, 0, 3, 4, 3, 3
	y, _ := NewCol([]int{1, -5, 6, 1, 3, 5, 4, 4}, d.DTint, d.ColName("y")) //6:  0, 1, 2, 0, 3, 4, 5, 5
	z, _ := NewCol([]string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310", "20160430", "20160430"}, d.DTstring, d.ColName("z"))
	dfx, e := NewDFcol(StandardFunctions(), []*Col{x, y, z})
	assert.Nil(t, e)
	dtx, ex := d.Parse(dfx, "date(z)")
	assert.Nil(t, ex)
	dt := dtx.AsColumn()
	d.ColName("dt")(dt)
	e = dfx.AppendColumn(dt, false)
	assert.Nil(t, e)

	var tab d.DF
	tab, e = dfx.Table(false, "x", "y")
	assert.Nil(t, e)
	cNames := tab.ColumnNames()
	for ind := 0; ind < len(cNames); ind++ {
		col := tab.Column(cNames[ind])
		fmt.Println(cNames[ind])
		fmt.Println(col.Data())
	}
}
