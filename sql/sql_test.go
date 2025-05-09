package sql

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	_ "github.com/jackc/pgx/stdlib"
)

// Create a dataframe from a ClickHouse table.
// On the loading side, there is no difference between Postgres and ClickHouse.
// There is when creating tables,
// see Dialect.Create and Dialect.Save.
//
// Note that this code is identical to the DBload example in df/mem.
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

// Create a dataframe from a Postgres table.
// On the loading side, there is no difference between Postgres and ClickHouse.
// There is when creating tables,
// see Dialect.Create and Dialect.Save.
func ExampleDBload_postgress() {
	const (
		dbProvider = "postgres"
		pgTable    = "d1"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	dbName := os.Getenv("db")
	db := newConnectPG(host, user, password, dbName)

	qry := "SELECT k, x FROM " + pgTable

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

// Create a new table grouping one one column with two summary columns.
func ExampleDF_By() {
	const n = 1000
	const (
		dbProvider = "clickhouse"
		chTable    = "testing.d1"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// df starts with 1 column, "seq", ranging from 0 to n
	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFseq(dlct, n, "seq"); e1 != nil {
		panic(e1)
	}

	// add some columns
	if e := d.Parse(df, "x := mod(seq, 10)"); e != nil {
		panic(e)
	}
	if e := d.Parse(df, "y := float(rowNumber())"); e != nil {
		panic(e)
	}
	var (
		dfBy d.DF
		e2   error
	)

	_ = dlct.Save("testing.temp", "seq", true, false, df)

	// produce a summary
	if dfBy, e2 = df.By("x", "cy := count(y)", "my := mean(y)"); e2 != nil {
		panic(e2)
	}

	if e := dfBy.Sort(true, "x"); e != nil {
		panic(e)
	}

	// These run a query to fetch the data
	fmt.Println(dfBy.Column("x").Data().AsAny())
	fmt.Println(dfBy.Column("cy").Data().AsAny())
	fmt.Println(dfBy.Column("my").Data().AsAny())
	// Output:
	// [0 1 2 3 4 5 6 7 8 9]
	// [100 100 100 100 100 100 100 100 100 100]
	// [495 496 497 498 499 500 501 502 503 504]
}

// Create a new table grouping one one column with two summary columns.
func ExampleDF_By_global() {
	const n = 1000
	const (
		dbProvider = "clickhouse"
		chTable    = "testing.d1"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// df starts with 1 column, "seq", ranging from 0 to n
	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFseq(dlct, n, "seq"); e1 != nil {
		panic(e1)
	}

	// add some columns
	if e := d.Parse(df, "x := mod(seq, 10)"); e != nil {
		panic(e)
	}
	if e := d.Parse(df, "y := float(rowNumber())"); e != nil {
		panic(e)
	}
	var (
		dfBy d.DF
		e2   error
	)

	_ = dlct.Save("testing.temp", "seq", true, false, df)

	// produce a summary
	if dfBy, e2 = df.By("x", "cy := count(y)", "prop := cy / count(global(y))"); e2 != nil {
		panic(e2)
	}

	if e := dfBy.Sort(true, "x"); e != nil {
		panic(e)
	}

	// These run a query to fetch the data
	fmt.Println(dfBy.Column("x").Data().AsAny())
	fmt.Println(dfBy.Column("cy").Data().AsAny())
	fmt.Println(dfBy.Column("prop").Data().AsAny())
	// Output:
	// [0 1 2 3 4 5 6 7 8 9]
	// [100 100 100 100 100 100 100 100 100 100]
	// [0.1 0.1 0.1 0.1 0.1 0.1 0.1 0.1 0.1 0.1]
}

// Create a new table grouping one one column with two summary columns.
func ExampleDF_By_oneRow() {
	const n = 1000
	const (
		dbProvider = "clickhouse"
		chTable    = "testing.d1"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// df starts with 1 column, "seq", ranging from 0 to n
	var (
		df *DF
		e1 error
	)
	if df, e1 = NewDFseq(dlct, n, "seq"); e1 != nil {
		panic(e1)
	}

	// add some columns
	if e := d.Parse(df, "x := mod(seq, 10)"); e != nil {
		panic(e)
	}
	if e := d.Parse(df, "y := float(rowNumber())"); e != nil {
		panic(e)
	}
	var (
		dfBy d.DF
		e2   error
	)

	_ = dlct.Save("testing.temp", "seq", true, false, df)

	// produce a summary
	if dfBy, e2 = df.By("", "cy := count(y)", "my := mean(y)"); e2 != nil {
		panic(e2)
	}

	qry := dfBy.Column("cy").(*Col).MakeQuery()
	_ = qry

	// These run a query to fetch the data
	fmt.Println(dfBy.Column("cy").Data().AsAny())
	fmt.Println(dfBy.Column("my").Data().AsAny())
	// Output:
	// [1000]
	// [499.5]
}

func ExampleDF_Interp() {
	const (
		n1         = 10
		dbProvider = "clickhouse"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// create first dataframe.
	x := make([]float64, n1)
	y := make([]float64, n1)
	for ind := range n1 {
		x[ind] = float64(ind)
		y[ind] = float64(ind) * 4
	}

	var (
		cx1, cy1 *m.Col
		e1       error
	)
	if cx1, e1 = m.NewCol(x, d.ColName("x")); e1 != nil {
		panic(e1)
	}
	if cy1, e1 = m.NewCol(y, d.ColName("y")); e1 != nil {
		panic(e1)
	}

	var (
		df1 *m.DF
		e2  error
	)
	if df1, e2 = m.NewDFcol([]*m.Col{cx1, cy1}); e2 != nil {
		panic(e2)
	}
	if e := dlct.Save("temp1", "x", true, false, df1); e != nil {
		panic(e)
	}

	var (
		df2 d.DF
		e3  error
	)
	if df2, e3 = DBload("select * from temp1", dlct); e3 != nil {
		panic(e3)
	}

	cxi := []float64{0.5, 4.25, -1, 20, 6.8}
	coli, _ := m.NewCol(cxi, d.ColName("xi"))
	var (
		dfi d.DF
		e4  error
	)
	if dfi, e4 = NewDF(dlct, coli); e4 != nil {
		panic(e4)
	}

	var (
		dfOut d.DF
		e5    error
	)
	if dfOut, e5 = df2.Interp(dfi, "x", "xi", "y", "yInterp"); e5 != nil {
		panic(e5)
	}

	fmt.Println(dfOut.Column("yInterp").Data().AsAny())
	// Output:
	// [2 17 27.2]
}

func ExampleDF_Join() {
	const (
		nLeft      = 10
		nRight     = 15
		dbProvider = "clickhouse"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	// initialize dialect
	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// create the dataframes to join
	var (
		dfLeft, dfRight d.DF
		e1              error
	)
	if dfLeft, e1 = NewDFseq(dlct, nLeft, "seq"); e1 != nil {
		panic(e1)
	}

	if dfRight, e1 = NewDFseq(dlct, nRight, "seq"); e1 != nil {
		panic(e1)
	}

	// add a column
	if e := d.Parse(dfLeft, "x := exp(float(seq) / 100.0)"); e != nil {
		panic(e)
	}

	if e := d.Parse(dfRight, "y := seq^2"); e != nil {
		panic(e)
	}

	// join
	var (
		dfJoin d.DF
		e2     error
	)
	if dfJoin, e2 = dfLeft.Join(dfRight, "seq"); e2 != nil {
		panic(e2)
	}

	fmt.Println(dfJoin.RowCount())
	fmt.Println(dfJoin.Column("seq").Data().AsAny())
	fmt.Println(dfJoin.Column("y").Data().AsAny())
	// Output:
	// 10
	// [0 1 2 3 4 5 6 7 8 9]
	// [0 1 4 9 16 25 36 49 64 81]
}

func ExampleDF_Join_twoColumns() {
	const (
		nLeft      = 10
		nRight     = 15
		dbProvider = "clickhouse"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	// initialize Dialect
	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	// Create the dataframes to join
	var (
		dfLeft, dfRight d.DF
		e1              error
	)
	if dfLeft, e1 = NewDFseq(dlct, nLeft, "seq"); e1 != nil {
		panic(e1)
	}

	if dfRight, e1 = NewDFseq(dlct, nRight, "seq"); e1 != nil {
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

	// join
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
	const (
		n          = 10
		dbProvider = "clickhouse"
	)

	// ClickHouse connection parameters.
	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db := newConnectCH(host, user, password)

	var (
		dlct *d.Dialect
		e0   error
	)
	if dlct, e0 = d.NewDialect(dbProvider, db); e0 != nil {
		panic(e0)
	}

	var (
		df d.DF
		e1 error
	)
	if df, e1 = NewDFseq(dlct, n, "seq"); e1 != nil {
		panic(e1)
	}

	if e := d.Parse(df, "x := 4.0 * float(seq)"); e != nil {
		panic(e)
	}

	// subset to where x <= 12.0 or x > 32.0
	dfOut, _ := df.Where("x <= 12.0 || x > 32.0")
	fmt.Println(dfOut.Column("x").Data().AsAny())
	// Output:
	// [0 4 8 12 36]
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

func newConnectPG(host, user, password, dbName string) *sql.DB {
	connectionStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, password, host, dbName)
	var (
		db *sql.DB
		e  error
	)
	if db, e = sql.Open("pgx", connectionStr); e != nil {
		panic(e)
	}

	if e := db.Ping(); e != nil {
		panic(e)
	}

	return db
}
