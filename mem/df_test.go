package df

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/df"

	"github.com/stretchr/testify/assert"
)

func makeMemDF() *MemDF {
	x, _ := NewMemCol("x", []float64{1, -2, 3, 0, 2, 3})
	y, _ := NewMemCol("y", []int{1, -5, 6, 1, 4, 5})
	z, _ := NewMemCol("z", []string{"20221231", "20000101", "19900615", "20220601", "20230915", "20060310"})
	dfx, e := NewMemDF(RunRowFn, RunDFfn, StandardFunctions(), x, y, z)
	_ = e
	xx, _ := NewMemCol("r", []int{1, 2, 3, 1, 2, 3})
	e = dfx.AppendColumn(xx, false)

	return dfx
}

func TestDF_Sort(t *testing.T) {
	dfx := makeMemDF()
	e := dfx.Sort("y", "z")
	assert.Nil(t, e)
	x, _ := dfx.Column("x")
	y, _ := dfx.Column("y")
	z, _ := dfx.Column("z")
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

func TestDBLoad(t *testing.T) {
	dfx := makeMemDF()
	eqn := " -3+b^3*4/(exp(x+y)*abs(zasdf,r)) + 3 + (b>=33)*((aa+bb)*(cc+DD))"
	//eqn = "3 + (b>=33)*((aa+bb)*(cc+DD))"
	//eqn = "-3*(((a+b))+1) + abs(3+4)"
	//	eqn = "-4*(-r+44)*a*b +/4"
	//	eqn = "a^b-c*cast('DTfloat',d)"
	//eqn = "a - b - c - abs(d)"
	//eqn = "4+exp(3+4, abs(4,4),3)"
	eqn = "ab:=3+2-2-x"
	eqn = "ab:= exp(1.0)*((1+2)*(3--2)/abs(-15.0)) "
	eqn = "ab := (3 * 4 + 1 - -1)*(2 + abs(-1.0))"
	eqn = "ab := y - (sum(y)-sum(x)-4)"
	eqn = "ab := sum(y)"
	//	eqn = "ab:=x+3"
	fmt.Println(eqn)
	col, e := df.ParseExpr(eqn, dfx)
	assert.Nil(t, e)
	fmt.Println(col.Data())
	fmt.Println(col.DataType())
	e = dfx.AppendColumn(col, true)
	assert.Nil(t, e)
}

func TestMemDF_Where(t *testing.T) {
	dfx := makeMemDF()
	eqn := "ind := x >= 1"
	ind, e := df.ParseExpr(eqn, dfx)
	assert.Nil(t, e)

	ey := dfx.Where(ind)
	assert.Nil(t, ey)
	assert.Equal(t, 4, dfx.RowCount())

	eqn = "ind := y >= 5"
	ind, e = df.ParseExpr(eqn, dfx)
	assert.Nil(t, e)

	ey = dfx.Where(ind)
	assert.Equal(t, 2, dfx.RowCount())

}

func TestParser(t *testing.T) {
	dfx := makeMemDF()
	eqn := "dt := date(z)"
	col, e := df.ParseExpr(eqn, dfx)
	dfx.AppendColumn(col, true)
	assert.Nil(t, e)

	x := [][]any{
		{"sum(y)", 0, int(12)},
		{"if(y != 1, 2.0, x)", 0, float64(1)},
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
		xOut, ex := df.ParseExpr("ab:="+eqn, dfx)
		assert.Nil(t, ex)

		indx := x[ind][1].(int)
		result := xOut.(*MemCol).Element(indx)

		_ = dfx.DropColumns("ab")
		if xOut.DataType() == df.DTfloat {
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
