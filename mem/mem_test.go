package mem

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	d "github.com/invertedv/df"
	"github.com/stretchr/testify/assert"
)

// Examples

func TestStart1(t *testing.T) {
	var (
		f  *d.Files
		e1 error
	)
	if f, e1 = d.NewFiles(d.FileStrict(true), d.FilePeek(500)); e1 != nil {
		panic(e1)
	}

	// this file is in df/data.
	fileToOpen := os.Getenv("datapath") + "dfExample.csv"
	// Since we haven't told Open about field names and types, it will read the first row as the header
	// and impute the data types.
	if ex := f.Open(fileToOpen); ex != nil {
		panic(ex)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = FileLoad(f); e2 != nil {
		panic(e2)
	}

	fmt.Println("A quick look at what we just read in:")
	fmt.Println(df)

	// using By with no grouping field produces a all-row summary
	dfSummA, e3 := df.By("", "n := count(dt)", "avgBal := mean(bal)", "nMarch := sum(if(dt==date('20250301'),1,0))")
	assert.Nil(t, e3)
	fmt.Println("Summary:")
	fmt.Println(dfSummA)

	var (
		dfSumm d.DF
		e4     error
	)
	// This creates a new dataframe grouping on age. For each age & dt combination, three fields are calculated:
	//  1. mb is the average balance within the age & dt.
	//  2. pAge is the percentage of the total balance in the file that has this age & dt value.
	//  3. dq is the percentage of balances at this age & dt that have status == 'D'.
	if dfSumm, e4 = df.By("age,dt", "mb := mean(bal)", "dq := 100.0 * sum(if(status=='D', bal, 0.0))/ sum(bal)", "balAgeDt := sum(bal)"); e4 != nil {
		panic(e4)
	}

	if ex := dfSumm.Sort(true, "age,dt"); ex != nil {
		panic(ex)
	}

	// now calculate the total balance by date
	var (
		dfSummDt d.DF
		e5       error
	)
	if dfSummDt, e5 = df.By("dt", "balDt := sum(bal)"); e5 != nil {
		panic(e5)
	}

	var (
		dfJoin d.DF
		e6 error
	)
	if dfJoin, e6 = dfSummDt.Join(dfSumm, "dt"); e6!=nil {
		panic(e6)
	}

	// pAge is the percentage of balances that are this age for this dt.
	if ex := d.Parse(dfJoin, "pAge := 100.0 * balAgeDt / balDt"); ex!=nil {
		panic(ex)
	}

	if ex := dfJoin.Sort(true, "age,dt"); ex!=nil{
		panic(ex)
	}

	fmt.Println("Summary by age and date")
	fmt.Println(dfJoin)

	// OK, let's save this...
	var (
		fs *d.Files
		e7 error
	)
	// Create a new Files struct to do this.
	// Write out the date, dt, in the format CCYYMMDD.
	if fs, e7 = d.NewFiles(d.FileDateFormat("20060102")); e7 != nil {
		panic(e7)
	}

	fileToSave := os.Getenv("datapath") + "dfSummary.csv"
	if ex := fs.Save(fileToSave, dfJoin); ex != nil {
		panic(ex)
	}

	fmt.Println(dfSumm.RowCount(), dfJoin.RowCount())
}

// Load a CSV with a header.  Column types are determined by peeking at the data.
func ExampleFileLoad() {
	var (
		f  *d.Files
		e1 error
	)
	if f, e1 = d.NewFiles(d.FileStrict(true)); e1 != nil {
		panic(e1)
	}

	// this file is in df/data.
	fileToOpen := os.Getenv("datapath") + "d1.csv"
	if ex := f.Open(fileToOpen); ex != nil {
		panic(ex)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = FileLoad(f); e2 != nil {
		panic(e2)
	}

	fmt.Println("# of Rows: ", df.RowCount())
	fmt.Println("Columns: ", df.ColumnNames())
	// Output:
	// # of Rows:  6
	// Columns:  [k x y yy z dt R]
}

// Load a CSV with a header.  Column names & types are specified by user.
// The source .CSV has a header, which is skipped.  Note, if you specify types,
// you must also specify names.
func ExampleFileLoad_types() {
	// ordered as in the file
	fieldNames := []string{"k", "x", "y", "yy", "z", "dt", "RNew"}
	fieldTypes := []d.DataTypes{d.DTint, d.DTfloat, d.DTint, d.DTint, d.DTstring, d.DTdate, d.DTfloat}

	var (
		f  *d.Files
		e1 error
	)
	if f, e1 = d.NewFiles(d.FileFieldNames(fieldNames), d.FileFieldTypes(fieldTypes)); e1 != nil {
		panic(e1)
	}

	fileToOpen := os.Getenv("datapath") + "d1.csv"
	if ex := f.Open(fileToOpen); ex != nil {
		panic(ex)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = FileLoad(f); e2 != nil {
		panic(e2)
	}

	ct, _ := df.ColumnTypes()
	fmt.Println(ct)
	// Output:
	// [DTint DTfloat DTint DTint DTstring DTdate DTfloat]
}

// Connect to ClickHouse and pull the data from a query.
// Note that this code is identical to the DBload example in df/sql.
// The mem/df package loads the data into memory, the sql/df package does not.
func ExampleDBload() {
	const (
		dbProvider = "clickhouse"
		chTable    = "testing.d1"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	qry := "SELECT k, x FROM " + chTable

	var (
		dlct *d.Dialect
		e    error
	)
	if dlct, e = d.NewDialect(dbProvider, db); e != nil {
		panic(e)
	}

	var (
		df *DF
		e1 error
	)
	if df, e1 = DBload(qry, dlct); e1 != nil {
		panic(e1)
	}

	fmt.Println("# of Rows: ", df.RowCount())
	fmt.Println("Columns: ", df.ColumnNames())
	// Output:
	// # of Rows:  6
	// Columns:  [k x]
}

// Create a new column from a *Vector.
func ExampleNewCol() {
	const n = 100

	x := make([]int64, n)
	for ind := range n {
		x[ind] = int64(ind * 2)
	}

	var (
		v  *d.Vector
		e1 error
	)
	// NewVector will convert the type.
	if v, e1 = d.NewVector(x, d.DTint); e1 != nil {
		panic(e1)
	}

	// Note, calling NewCol with x will generate an error since x is not of type int
	// and NewCol does not convert types.
	var (
		col *Col
		e2  error
	)
	if col, e2 = NewCol(v, d.ColName("x")); e2 != nil {
		panic(e2)
	}

	fmt.Println(col.AsAny().([]int)[0:10])
	// Output:
	// [0 2 4 6 8 10 12 14 16 18]
}

// Create columns from slices and then create a new dataframe from them
func ExampleNewDFcol() {
	const n = 100

	x := make([]int, n)
	y := make([]float64, n)
	for ind := range n {
		x[ind] = ind * 2
		y[ind] = float64(x[ind])
	}

	var (
		col1, col2 *Col
		e1         error
	)

	if col1, e1 = NewCol(x, d.ColName("x")); e1 != nil {
		panic(e1)
	}

	if col2, e1 = NewCol(x, d.ColName("y")); e1 != nil {
		panic(e1)
	}

	var (
		df *DF
		e2 error
	)
	if df, e2 = NewDFcol([]*Col{col1, col2}); e2 != nil {
		panic(e2)
	}

	var (
		xf []float64
		e3 error
	)
	// This will convert x to a float64.
	if xf, e3 = df.Column("x").Data().AsFloat(); e3 != nil {
		panic(e3)
	}

	fmt.Println(xf[0:10])

	// Output:
	// [0 2 4 6 8 10 12 14 16 18]
}

// Append a column to a dataframe
func ExampleDF_AppendColumn() {
	const (
		n    = 100
		slen = 4
	)

	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFseq(n); e1 != nil {
		panic(e1)
	}

	x := make([]string, n)
	for ind := range n {
		x[ind] = d.RandomLetters(4)
	}

	// create a column named "x" from x.
	var (
		col *Col
		e2  error
	)
	if col, e2 = NewCol(x, d.ColName("x")); e2 != nil {
		panic(e2)
	}

	if e := df.AppendColumn(col, false); e != nil {
		panic(e)
	}

	fmt.Println(df.ColumnNames())

	// Output:
	// [seq x]
}

// Create a new table grouping one one column with two summary columns.
func ExampleDF_By() {
	const n = 1000

	// create source dataframe.
	x := make([]int, n)
	y := make([]float64, n)
	for ind := range n {
		x[ind] = ind % 4
		y[ind] = float64(ind)
	}

	var (
		cx, cy *Col
		e0     error
	)
	if cx, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFcol([]*Col{cx, cy}); e1 != nil {
		panic(e1)
	}
	var (
		dfBy d.DF
		e2   error
	)

	// produce a summary
	if dfBy, e2 = df.By("x", "my := mean(y)", "sy := sum(y)"); e2 != nil {
		panic(e2)
	}

	if e := dfBy.Sort(true, "x"); e != nil {
		panic(e)
	}

	fmt.Println(dfBy.Column("x").Data().AsAny())
	fmt.Println(dfBy.Column("my").Data().AsAny())
	fmt.Println(dfBy.Column("sy").Data().AsAny())
	// Output:
	// [0 1 2 3]
	// [498 499 500 501]
	// [124500 124750 125000 125250]
}

// Create a new table grouping on two columns with two summary columns.
func ExampleDF_By_twoColumns() {
	const n = 1000

	// create source dataframe.
	x := make([]int, n)
	r := make([]int, n)
	y := make([]float64, n)
	for ind := range n {
		x[ind] = ind % 4
		r[ind] = ind % 8
		y[ind] = float64(ind)
	}

	var (
		cx, cr, cy *Col
		e0         error
	)
	if cx, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cr, e0 = NewCol(r, d.ColName("r")); e0 != nil {
		panic(e0)
	}
	if cy, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFcol([]*Col{cx, cr, cy}); e1 != nil {
		panic(e1)
	}
	var (
		dfBy d.DF
		e2   error
	)

	// produce a summary
	if dfBy, e2 = df.By("x,r", "my := mean(y)", "sy := sum(y)"); e2 != nil {
		panic(e2)
	}

	if e := dfBy.Sort(true, "x,r"); e != nil {
		panic(e)
	}

	fmt.Println(dfBy.Column("x").Data().AsAny())
	fmt.Println(dfBy.Column("r").Data().AsAny())
	fmt.Println(dfBy.Column("my").Data().AsAny())
	fmt.Println(dfBy.Column("sy").Data().AsAny())
	// Output:
	// [0 0 1 1 2 2 3 3]
	// [0 4 1 5 2 6 3 7]
	// [496 500 497 501 498 502 499 503]
	// [62000 62500 62125 62625 62250 62750 62375 62875]
}

// Create a summary table that requires a global summary in the calculation.
func ExampleDF_By_global() {
	const n = 1000

	// create source dataframe.
	x := make([]int, n)
	y := make([]float64, n)
	for ind := range n {
		x[ind] = ind % 4
		y[ind] = float64(ind)
	}

	var (
		cx, cy *Col
		e0     error
	)
	if cx, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFcol([]*Col{cx, cy}); e1 != nil {
		panic(e1)
	}
	var (
		dfBy d.DF
		e2   error
	)
	// produce a summary
	if dfBy, e2 = df.By("x", "cnt := count(x)", "total := count(global(x))", "prop := 100.0 * float(cnt)/float(total)"); e2 != nil {
		panic(e2)
	}
	//	if dfBy, e2 = df.By("x", "cnt := count(x)", "prop := float(cnt)/float(count(global(x)))"); e2 != nil {
	//		panic(e2)
	//	}

	if e := dfBy.Sort(true, "x"); e != nil {
		panic(e)
	}

	fmt.Println(dfBy.Column("x").Data().AsAny())
	fmt.Println(dfBy.Column("cnt").Data().AsAny())
	fmt.Println(dfBy.Column("total").Data().AsAny())
	fmt.Println(dfBy.Column("prop").Data().AsAny())
	//
	// Output:
	// [0 1 2 3]
	// [250 250 250 250]
	// [1000 1000 1000 1000]
	// [25 25 25 25]
}

// Create a summary with no grouping column.
func ExampleDF_By_oneRow() {
	const n = 1000

	// create source dataframe.
	x := make([]int, n)
	y := make([]float64, n)
	for ind := range n {
		x[ind] = ind % 4
		y[ind] = float64(ind)
	}

	var (
		cx, cy *Col
		e0     error
	)
	if cx, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFcol([]*Col{cx, cy}); e1 != nil {
		panic(e1)
	}
	var (
		dfBy d.DF
		e2   error
	)
	// produce a summary
	if dfBy, e2 = df.By("", "cnt := count(y)", "sy := sum(y)"); e2 != nil {
		panic(e2)
	}

	fmt.Println(dfBy.Column("cnt").Data().AsAny())
	fmt.Println(dfBy.Column("sy").Data().AsAny())
	// Output:
	// [1000]
	// [499500]
}

func ExampleDF_Interp() {
	const n1 = 10

	// create first dataframe.
	x := make([]float64, n1)
	y := make([]float64, n1)
	for ind := range n1 {
		x[ind] = float64(ind)
		y[ind] = float64(ind) * 4
	}

	var (
		cx1, cy1 *Col
		e0       error
	)
	if cx1, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy1, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df1 *DF
		e1  error
	)
	if df1, e1 = NewDFcol([]*Col{cx1, cy1}); e1 != nil {
		panic(e1)
	}

	cxi := []float64{0.5, 4.25, -1, 20, 6.8}
	coli, _ := NewCol(cxi, d.ColName("xi"))

	dfOut, _ := df1.Interp(coli, "x", "xi", "y", "yInterp")
	fmt.Println(dfOut.Column("yInterp").Data().AsAny())
	// Output:
	// [2 17 27.2]
}

func ExampleDF_Join() {
	const (
		n1 = 10
		n2 = 15
	)

	// create first dataframe.
	x := make([]int, n1)
	y := make([]float64, n1)
	for ind := range n1 {
		x[ind] = ind
		y[ind] = float64(ind) * 4
	}

	var (
		cx1, cy1 *Col
		e0       error
	)
	if cx1, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy1, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df1 *DF
		e1  error
	)
	if df1, e1 = NewDFcol([]*Col{cx1, cy1}); e1 != nil {
		panic(e1)
	}

	// create second dataframe.
	x = make([]int, n2)
	z := make([]float64, n2)
	for ind := range n2 {
		x[ind] = ind
		z[ind] = -float64(ind) * 4
	}

	var (
		cx2, cz2 *Col
		e2       error
	)
	if cx2, e2 = NewCol(x, d.ColName("x")); e2 != nil {
		panic(e2)
	}
	if cz2, e2 = NewCol(z, d.ColName("z")); e2 != nil {
		panic(e2)
	}

	var (
		df2 *DF
		e3  error
	)
	if df2, e3 = NewDFcol([]*Col{cx2, cz2}); e3 != nil {
		panic(e3)
	}

	var (
		dfJoin d.DF
		e4     error
	)
	if dfJoin, e4 = df1.Join(df2, "x"); e4 != nil {
		panic(e4)
	}
	fmt.Println(dfJoin.Column("x").Data().AsAny())
	fmt.Println(dfJoin.Column("y").Data().AsAny())
	fmt.Println(dfJoin.Column("z").Data().AsAny())
	// Output:
	// [0 1 2 3 4 5 6 7 8 9]
	// [0 4 8 12 16 20 24 28 32 36]
	// [-0 -4 -8 -12 -16 -20 -24 -28 -32 -36]
}

// Join based on two columns.  Compare to the same example under df/sql.
func ExampleDF_Join_twoColumns() {
	const (
		nLeft      = 10
		nRight     = 15
		dbProvider = "clickhouse"
	)

	var (
		dfLeft, dfRight d.DF
		e1              error
	)
	if dfLeft, e1 = NewDFseq(nLeft); e1 != nil {
		panic(e1)
	}

	if dfRight, e1 = NewDFseq(nRight); e1 != nil {
		panic(e1)
	}

	// second column to join on
	if e := d.Parse(dfLeft, "b := if(mod(seq,4) == 0, 'a', if(mod(seq,4)==1, 'b', if(mod(seq,4)==2, 'c', 'd')))"); e != nil {
		panic(e)
	}

	if e := d.Parse(dfRight, "b := if(mod(seq,4) == 0, 'a', 'b')"); e != nil {
		panic(e)
	}

	// add another column to each
	if e := d.Parse(dfLeft, "x := exp(float(seq) / 100.0)"); e != nil {
		panic(e)
	}

	if e := d.Parse(dfRight, "y := seq^2"); e != nil {
		panic(e)
	}

	var (
		dfJoin d.DF
		e2     error
	)

	if dfJoin, e2 = dfLeft.Join(dfRight, "seq,b"); e2 != nil {
		panic(e2)
	}

	fmt.Println(dfJoin.RowCount())
	fmt.Println(dfJoin.Column("seq").Data().AsAny())
	fmt.Println(dfJoin.Column("b").Data().AsAny())
	fmt.Println(dfJoin.Column("y").Data().AsAny())
	// Output:
	// 6
	// [0 1 4 5 8 9]
	// [a b a b a b]
	// [0 1 16 25 64 81]
}

func ExampleDF_Where() {
	const n1 = 10

	// create dataframe.
	x := make([]int, n1)
	y := make([]float64, n1)
	for ind := range n1 {
		x[ind] = ind
		y[ind] = float64(ind) * 4
	}

	var (
		cx1, cy1 *Col
		e0       error
	)
	if cx1, e0 = NewCol(x, d.ColName("x")); e0 != nil {
		panic(e0)
	}
	if cy1, e0 = NewCol(y, d.ColName("y")); e0 != nil {
		panic(e0)
	}

	var (
		df1 *DF
		e1  error
	)
	if df1, e1 = NewDFcol([]*Col{cx1, cy1}); e1 != nil {
		panic(e1)
	}

	// subset to where x < 4 or x > 8
	dfOut, _ := df1.Where("x < 4 || x > 8")
	fmt.Println(dfOut.Column("x").Data().AsAny())
	// Output:
	// [0 1 2 3 9]

}

// NewConnect established a new connection to ClickHouse.
// host is IP address (assumes port 9000), memory is max_memory_usage
func newConnectCH(host, user, password string) *sql.DB {
	db := clickhouse.OpenDB(
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

	if e := db.Ping(); e != nil {
		panic(e)
	}

	return db
}
