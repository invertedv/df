package df

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	u "github.com/invertedv/utilities"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/df"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, 3})
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	yy, _ := NewMemCol("yy", []int{1, -15, 16, 1, 4, 5})
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310"})
	dfx, e := NewMemDF(RunRowFn, RunDFfn, StandardFunctions(), x, y, z, yy)
	_ = e
	xx, _ := NewMemCol("r", []int{1, 2, 3, 1, 2, 3})
	e = dfx.AppendColumn(xx, false)

	return dfx
}

func TestDF_Sort(t *testing.T) {
	dfx := makeMemDF()
	e := dfx.Sort(true, "y", "z")
	assert.Nil(t, e)
	x, _ := dfx.Column("x")
	y, _ := dfx.Column("y")
	z, _ := dfx.Column("z")
	fmt.Println(x.Data(), y.Data(), z.Data())
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
	memDF, e1 := DBLoad("SELECT * FROM zip.zip3 LIMIT 10", dialect)
	assert.Nil(t, e1)
	col, e2 := memDF.Column("prop_zip3")
	assert.Nil(t, e2)
	fmt.Println(col.Data())
	e = memDF.Apply("abc", "cast", false, "DTstring", "abc")
	assert.Nil(t, e)
	e = memDF.Apply("abcd", "cast", false, "DTdate", "March 25, 1990")
	assert.Nil(t, e)
	e = memDF.Apply("abd", "cast", false, "DTstring", "latitude")
	assert.Nil(t, e)

	ed := memDF.CreateTable("tmp.aaa", "prop_zip3", true, "prop_zip3", "latitude")
	assert.Nil(t, ed)
	fmt.Println("len", memDF.Len())
	ed = memDF.FileSave("/home/will/tmp/test.csv")
	assert.Nil(t, ed)
}

func TestWhere(t *testing.T) {
	dfx := makeMemDF()
	expr := "where(y>1)"
	outDF, e := df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, e)
	_ = outDF
}

func TestMemDF_Where(t *testing.T) {
	dfx := makeMemDF()
	eqn := "x >= 1"
	ind, e := df.ParseExpr(eqn, dfx.DFcore)
	assert.Nil(t, e)

	dfNew, ey := dfx.Where(ind.AsColumn())
	assert.Nil(t, ey)
	assert.Equal(t, 4, dfNew.RowCount())

	eqn = "y >= 5"
	ind, e = dfNew.Parse(eqn)
	assert.Nil(t, e)

	dfNew2, ez := dfNew.Where(ind.AsColumn())
	assert.Nil(t, ez)
	assert.Equal(t, 2, dfNew2.RowCount())

}

func TestParser(t *testing.T) {
	dfx := makeMemDF()
	eqn := "date(z)"
	colx, e := df.ParseExpr(eqn, dfx.DFcore)
	col := colx.AsColumn()
	col.Name("dt")
	dfx.AppendColumn(col, true)
	assert.Nil(t, e)

	x := [][]any{
		{"if(y != 1, 2.0, (x))", 0, float64(1)},
		{"sum(y)", 0, int(12)},
		{"y != 1", 0, int(0)},
		{"y>=1 && y>=1 && dt >= date(20221231)", 0, int(1)},
		{"y>=1 && y>=1 && dt > date(20221231)", 0, int(0)},
		{"y>=1 && y>=1", 0, int(1)},
		{"!(y>=1) && y>=1", 0, int(0)},
		{"!1 && 1 || 1", 0, int(1)},
		{"!1 && 1 || 0", 0, int(0)},
		{"!0 && 1 || 0", 0, int(1)},
		{"!1 && 1", 0, int(0)},
		{"1 || 0 && 1", 0, int(1)},
		{"0 || 0 && 1", 0, int(0)},
		{"0 || 1 && 1", 0, int(1)},
		{"0 || 1 && 1 && 0", 0, int(0)},
		{"(0 || 1 && 1) && 0", 0, int(0)},
		{"y < 2", 0, int(1)},
		{"y < 1", 0, int(0)},
		{"y <= 1", 0, int(1)},
		{"y > 1", 0, int(0)},
		{"y >= 1", 0, int(1)},
		{"dt != date(20221231)", 0, int(0)},
		{"dt != date(20221231)", 1, int(1)},
		{"dt == date(20221231)", 0, int(1)},
		{"dt == date(20221231)", 1, int(0)},
		{"y == 1", 0, int(1)},
		{"y == 1", 1, int(0)},
		{"y && 1", 0, int(1)},
		{"0 && 1", 0, int(0)},
		{"0 || 0", 0, int(0)},
		{"0 || 1", 0, int(1)},
		{"4+3", 0, int(7)},
		{"4-1-1-1-1", 0, int(0)},
		{"4+1-1", 0, int(4)},
		{"4+1--1", 0, int(6)},
		{"float(4)+1--1", 0, float64(6)},
		{"((4+2) * abs(-3/2.0))", 0, float64(9)},
		{"exp(1.0)*abs(float(-2/(1+1)))", 0, math.Exp(1)},
		{"cast('DTdate', 20020630)", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
		{"date( 20020630)", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
		{"date('2002-06-30')", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
		{"'ab' + 'cd'", 0, "abcd"},
		{"((exp(1.0) + log(exp(1.0))))*(3--1)", 0, 4.0 + 4.0*math.Exp(1)},
		{"-x +2", 0, float64(1)},
		{"-x +4", 1, float64(6)},
		{"x/0", 0, math.Inf(1)},
		{"(3 * 4 + 1 - -1)*(2 + abs(-1.0))", 0, float64(42)},
		{"(1 + 2) - -(-1 - 2)", 0, int(0)},
		{"(1.0 + 3.0) / abs(-(-1.0 + 3.0))", 0, float64(2)},
		{"string(float(1))", 0, "1.00"},
		{"float('1.1')", 0, float64(1.1)},
		{"int(2.9)", 0, int(2)},
	}

	cnt := 0
	for ind := 0; ind < len(x); ind++ {
		cnt++
		eqn := x[ind][0].(string)
		fmt.Println(eqn)
		//xOut, ex := df.ParseExpr(eqn, dfx)
		//		xOut, ex := df.Parse(eqn, dfx.DFcore)
		xOut, ex := dfx.Parse(eqn)
		assert.Nil(t, ex)

		indx := x[ind][1].(int)
		result := xOut.AsColumn().(*MemCol).Element(indx)

		_ = dfx.DropColumns("ab")
		if xOut.AsColumn().DataType() == df.DTfloat {
			assert.InEpsilon(t, result.(float64), x[ind][2].(float64), .001)
			continue
		}

		assert.Equal(t, result, x[ind][2])
	}

	fmt.Println("# tests: ", cnt)
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
	dfx := makeMemDF()
	dfy := makeMemDF()

	dfz, e := dfx.AppendDF(dfy)
	assert.Nil(t, e)
	var col df.Column
	col, e = dfz.Column("x")
	assert.Nil(t, e)
	assert.Equal(t, float64(1), col.(*MemCol).Element(0))
	assert.Equal(t, float64(1), col.(*MemCol).Element(dfx.RowCount()))
}

func TestToCat(t *testing.T) {
	//	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	// z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310"})
	dfx := makeMemDF()
	expr := "date(z)"
	var (
		colx *df.Parsed
		ex   error
	)
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	col := colx.AsColumn()
	col.Name("dt")
	ex = dfx.AppendColumn(col, false)
	assert.Nil(t, ex)

	expr = "cat(y)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	exp := []int{0, 1, 2, 0, 3, 4}
	assert.Nil(t, ex)
	assert.Equal(t, exp, colx.AsColumn().Data())

	expr = "cat(y,  +1, -5)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	exp = []int{0, 1, -1, 0, -1, -1}
	assert.Equal(t, exp, colx.AsColumn().Data())

	expr = "cat(z)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	exp = []int{0, 1, 2, 2, 3, 4}
	assert.Equal(t, exp, colx.AsColumn().Data())

	expr = "cat(dt)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	exp = []int{0, 1, 2, 2, 3, 4}
	assert.Equal(t, exp, colx.AsColumn().Data())

	expr = "cat(x)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.NotNil(t, ex)
}

func TestApplyCat(t *testing.T) {
	//	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	//	yy, _ := NewMemCol("yy", []int{1, -15, 16, 1, 4, 5})
	dfx := makeMemDF()
	expr := "cat(y)"
	colx, ex := df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	col := colx.AsColumn()
	col.Name("c")
	_ = dfx.AppendColumn(col, false)

	expr = "applyCat(yy, c, 100)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	exp := []int{0, -1, -1, 0, 3, 4}
	assert.Equal(t, exp, colx.AsColumn().Data())

	// TODO: think about -- this works
	expr = "c + y"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	fmt.Println(colx.AsColumn().Data())
}

func TestFuzzCat(t *testing.T) {
	dfx := makeMemDF()
	expr := "cat(y)"
	colx, ex := df.ParseExpr(expr, dfx.DFcore)
	col := colx.AsColumn()
	col.Name("c")
	assert.Nil(t, ex)
	_ = dfx.AppendColumn(col, false)

	expr = "fuzzCat(c, 2, 100)"
	colx, ex = df.ParseExpr(expr, dfx.DFcore)
	assert.Nil(t, ex)
	exp := []int{0, -1, -1, 0, -1, -1}
	assert.Equal(t, exp, colx.AsColumn().Data())

}

func TestXYZ(t *testing.T) {
	x, _ := NewMemCol("x", []int{1, -5, 6, 1, 4, 5, 4, 4}) //5:  0, 1, 2, 0, 3, 4, 3, 3
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 3, 5, 4, 4}) //6:  0, 1, 2, 0, 3, 4, 5, 5
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310", "20160430", "20160430"})
	/*
		0, 0 -> 1
		1, 1 -> 2
		2, 2 -> 3
		0, 0 -> 1
		3, 3 -> 4
		4, 4 -> 5
		3, 5 -> 6
		3, 5 -> 6

	*/
	_ = y
	_ = z

	cols := makeTable(x, z)
	for c := 0; c < len(cols); c++ {
		fmt.Println(cols[c].Data())
	}

	var d []int
	var e []string
	for k := 0; k < 10000; k++ {
		d = append(d, k%100000)
		e = append(e, u.RandomLetters(1))
	}

	start := time.Now()
	fmt.Println(start)
	dc, _ := NewMemCol("dc", d)
	ec, _ := NewMemCol("ec", e)
	cols = makeTable(dc, ec)
	fmt.Println(cols[0].Len())
	//	for c := 0; c < len(cols); c++ {
	//		fmt.Println(cols[c].Data())
	//	}

	fmt.Println(time.Since(start).Seconds(), " seconds")

}

func TestMemDF_Table(t *testing.T) {
	x, _ := NewMemCol("x", []int{1, -5, 6, 1, 4, 5, 4, 4}) //5:  0, 1, 2, 0, 3, 4, 3, 3
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 3, 5, 4, 4}) //6:  0, 1, 2, 0, 3, 4, 5, 5
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "20060102", "20060102", "20230915", "20060310", "20160430", "20160430"})
	dfx, e := NewMemDF(RunRowFn, RunDFfn, StandardFunctions(), x, y, z)
	assert.Nil(t, e)
	dtx, ex := df.ParseExpr("date(z)", dfx.DFcore)
	assert.Nil(t, ex)
	dt := dtx.AsColumn()
	dt.Name("dt")
	e = dfx.AppendColumn(dt, false)
	assert.Nil(t, e)

	var tab df.DF
	tab, e = dfx.Table(false, "x", "y")
	cNames := tab.ColumnNames()
	for ind := 0; ind < len(cNames); ind++ {
		col, _ := tab.Column(cNames[ind])
		fmt.Println(cNames[ind])
		fmt.Println(col.Data())
	}
}
