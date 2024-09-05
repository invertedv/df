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
	y, _ := NewMemCol("y", []int{1, 5, 6, 1, 4, 5})
	z, _ := NewMemCol("z", []string{"p20221231", "20000101", "19900615", "20220601", "20230915", "20060310"})
	dfx, e := NewMemDF(Run, StandardFunctions(), x, y, z)
	_ = e
	xx, _ := NewMemCol("r", []int{1, 2, 3, 1, 2, 3})
	e = dfx.AppendColumn(xx)

	return dfx
}

func TestDF_Apply(t *testing.T) {
	df := makeMemDF()

	ee := df.Apply("fir", "if", "==", "z", "p20221231")
	assert.Nil(t, ee)
	ce, _ := df.Column("fir")
	fmt.Println("if", ce.Data())

	e2 := df.Apply("c", "c", "DTstring", "1")
	assert.Nil(t, e2)
	cc, _ := df.Column("c")
	fmt.Println(cc.Data())

	e2 = df.Apply("cx", "exp", "1")
	assert.Nil(t, e2)
	cc, _ = df.Column("cx")
	fmt.Println("exp", cc.Data())

	e2 = df.Apply("cxa", "add", "10", "y")
	assert.Nil(t, e2)
	cc, _ = df.Column("cxa")
	fmt.Println("cx", cc.DataType(), cc.Data())

	e1x := df.Apply("aexp", "exp", "1")
	assert.Nil(t, e1x)
	cc, _ = df.Column("aexp")
	fmt.Println("constant ", cc.Data())
	fmt.Println("row count: ", df.RowCount())
	e1x = df.Sort("x")
	assert.Nil(t, e1x)
	cc, _ = df.Column("x")
	fmt.Println("sorted x", cc.Data())
	cc, _ = df.Column("z")
	fmt.Println("sorted z", cc.Data())

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

	c, _ = df.Column("aexp")
	fmt.Println(c.Data())
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
	memDF, e1 := DBLoad("SELECT * FROM zip.zip3 LIMIT 10", dialect)
	assert.Nil(t, e1)
	col, e2 := memDF.Column("prop_zip3")
	assert.Nil(t, e2)
	fmt.Println(col.Data())
	e = memDF.Apply("abc", "cast", "DTstring", "abc")
	assert.Nil(t, e)
	e = memDF.Apply("abcd", "cast", "DTdate", "March 25, 1990")
	assert.Nil(t, e)
	e = memDF.Apply("abd", "cast", "DTstring", "latitude")
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
	//	eqn = "ab:=x+3"
	fmt.Println(eqn)
	e := df.Parse(eqn, dfx)
	assert.Nil(t, e)
	col, ex := dfx.Column("ab")
	assert.Nil(t, ex)
	fmt.Println(col.Data())
}

func TestParser(t *testing.T) {
	x := [][]any{
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
	}

	dfx := makeMemDF()

	cnt := 0
	for ind := 0; ind < len(x); ind++ {
		cnt++
		eqn := x[ind][0].(string)
		e := df.Parse("ab:="+eqn, dfx)
		assert.Nil(t, e)
		xOut, ex := dfx.Column("ab")
		assert.Nil(t, ex)

		indx := x[ind][1].(int)
		var result any
		switch r := xOut.Data().(type) {
		case []int:
			result = r[indx]
		case []float64:
			result = r[indx]
		case []string:
			result = r[indx]
		case []time.Time:
			result = r[indx]
		default:
			panic("failed type")
		}

		_ = dfx.DropColumns("ab")
		if xOut.DataType() == df.DTfloat {
			//			fmt.Println(result.(float64), x[ind][2].(float64))
			assert.InEpsilon(t, result.(float64), x[ind][2].(float64), .001)
			continue
		}

		assert.Equal(t, result, x[ind][2])
	}

	fmt.Println("# tests: ", cnt)
}
