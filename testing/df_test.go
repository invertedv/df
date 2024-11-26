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

// THINK about...how self interacts in context...
// CONSIDER making .Data fetch the data for sql....
const (
	dbSource   = "clickhouse"
	fileName   = "test.csv"
	fileNameW1 = "testFW.txt"
	fileNameW2 = "testFW1.txt"
	fileNameW3 = "testFW2.txt"
	inTable    = "testing.d1"
	outTable   = "testing.test"

	ch = "clickhouse"
)

// environment variables:
//   - host ClickHouse IP address
//   - user ClickHouse user
//   - password: ClickHouse password
//   - datapath: path to data directory in this project

// list of packages to test
func pkgs() []string {
	return []string{"sql", "mem"}
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

func loadData(pkg string) d.DF {
	const table = "SELECT * FROM " + inTable
	var db *sql.DB

	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")

	switch dbSource {
	case ch:
		db = newConnectCH(host, user, password)
	default:
		panic("unsupported database")
	}

	var (
		dialect *d.Dialect
		e       error
	)
	if dialect, e = d.NewDialect("clickhouse", db); e != nil {
		panic(e)
	}

	ctx := d.NewContext(dialect, nil, nil)

	if pkg == "sql" {
		var (
			df *s.DF
			e1 error
		)
		if df, e1 = s.DBload(table, ctx); e1 != nil {
			panic(e1)
		}
		return df
	}

	var (
		df *m.DF
		e2 error
	)
	if df, e2 = m.DBLoad(table, dialect); e2 != nil {
		panic(e2)
	}

	df.SetContext(ctx)
	df.Context().SetSelf(df)

	return df
}

func checker(df d.DF, colName, which string, col d.Column, indx int) any {
	if col != nil {
		col.Rename(colName)
		if e := df.AppendColumn(col, true); e != nil {
			panic(e)
		}
	}
	var colRet d.Column

	if which == "mem" {
		colRet = df.(*m.DF).Column(colName)
	}

	if which == "sql" {
		memDF, e1 := m.DBLoad(df.(*s.DF).MakeQuery(), df.Context().Dialect())
		if e1 != nil {
			panic(e1)
		}

		colRet = memDF.Column(colName)
	}

	if indx < 0 {
		return colRet.Data()
	}

	if x := colRet.(*m.Col).Element(indx); x != nil {
		return x
	}

	panic(fmt.Errorf("error in checker"))
}

/*
	func TestPlotXY(t *testing.T) {
		dfx := loadData("sql")
		e := dfx.Sort(true, "x")
		assert.Nil(t, e)
		p := d.NewPlot(d.WithTitle("This Is A Test"), d.WithXlabel("X-Axis"),
			d.WithYlabel("Y-Axis"), d.WithLegend(true))
		d.WithSubtitle("(subtitle here)")(p)
		d.WithXlabel("New X Label")(p)
		d.WithTitle("What???")(p)
		d.WithHeight(800)(p)
		d.WithWidth(800)(p)
		x := dfx.Column("x")
		y, _ := dfx.Parse("exp(x)")
		y.AsColumn().Name("expy")
		e1 := p.PlotXY(x, y.AsColumn(), "s1", "red")
		assert.Nil(t, e1)
		e2 := p.PlotXY(x, x, "s2", "black")
		assert.Nil(t, e2)
		//	e3 := p.Show("", "")
		//	assert.Nil(t, e3)
	}
*/

func TestTypes(t *testing.T) {
	table := "SELECT * EXCEPT(fhfa_msad, delta) FROM fhfa.msad LIMIT 10"
	var db *sql.DB

	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")

	switch dbSource {
	case ch:
		db = newConnectCH(host, user, password)
	default:
		panic("unsupported database")
	}

	var (
		dialect *d.Dialect
		e       error
	)
	if dialect, e = d.NewDialect("clickhouse", db); e != nil {
		panic(e)
	}
	ctx := d.NewContext(dialect, nil, nil)

	var (
		df *s.DF
		e1 error
	)
	if df, e1 = s.DBload(table, ctx); e1 != nil {
		panic(e1)
	}
	fmt.Println(df.Column("yr").Name())
	fmt.Println(df.Column("yr").Data())
}

// TODO: if just return "x" in parse, it returns a pointer to the original not a copy
func TestCheck(t *testing.T) {
	dfx := loadData("sql")
	dfy := loadData("sql")
	out, e := dfx.Parse("float(x)")
	assert.Nil(t, e)
	_ = out.AsColumn().Rename("xx")
	e1 := dfx.AppendColumn(out.AsColumn(), false)
	assert.Nil(t, e1)
	e2 := dfy.AppendColumn(out.AsColumn(), false)
	assert.Nil(t, e2)
}

func TestRename(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		x1 := dfx.Column("x").Data()
		e1 := dfx.Column("x").Rename("xa")
		assert.Nil(t, e1)
		e2 := dfx.Column("xa").Rename("xb")
		assert.Nil(t, e2)
		assert.Equal(t, x1, dfx.Column("xb").Data())
		e3 := dfx.Column("xb").Rename("x!")
		assert.NotNil(t, e3)
	}
}

func TestString(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		fmt.Println(dfx)
	}
}

func TestRowNumber(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		out, e := dfx.Parse("rowNumber()")
		assert.Nil(t, e)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, out.AsColumn().Data())
	}
}

func TestSeq(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		var df d.DF
		switch which {
		case "mem":
			df = m.NewDFseq(nil, nil, 5)
		default:
			df = s.NewDFseq(nil, dfx.Context(), 5)
		}

		col := df.Column("seq")
		assert.NotNil(t, col)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, col.Data())
	}
}

func TestSQLsave(t *testing.T) {
	const coln = "x"

	for _, which := range pkgs() {
		dfx := loadData(which)
		dlct := dfx.Context().Dialect()

		// save to a table
		e := dlct.Save(outTable, "k", true, dfx)
		assert.Nil(t, e)
		c1 := dfx.Column(coln)
		assert.NotNil(t, c1)

		// if this is sql, populate a mem DF to get values
		if which == "sql" {
			dfz, ez := m.DBLoad(c1.(*s.Col).MakeQuery(), dfx.Context().Dialect())
			assert.Nil(t, ez)
			c1 = dfz.Column(coln)
			assert.NotNil(t, c1)
		}

		c1.Rename("expected")

		// pull back from database
		dfy, ex := m.DBLoad("SELECT * FROM "+outTable, dfx.Context().Dialect())
		assert.Nil(t, ex)
		c2 := dfy.Column(coln)
		assert.NotNil(t, c2)
		c2.Rename("actual")

		// join expected & actual into a dataframe
		ctx := d.NewContext(dfx.Context().Dialect(), nil)
		dfb, eb := m.NewDFcol(nil, ctx, c1.(*m.Col), c2.(*m.Col))
		assert.Nil(t, eb)
		outx, ep := dfb.Parse("actual==expected")
		assert.Nil(t, ep)
		assert.Equal(t, []int{1, 1, 1, 1, 1, 1}, outx.AsColumn().Data())
	}
}

func TestFileSave(t *testing.T) {
	const coln = "x"

	for _, which := range pkgs() {
		dfx := loadData(which)
		f := d.NewFiles()

		fn := d.Slash(os.Getenv("datapath")) + fileName
		e := f.Save(fn, dfx)
		assert.Nil(t, e)

		ct, _ := dfx.ColumnTypes()
		e1 := f.Open(fn, dfx.ColumnNames(), ct, nil)
		assert.Nil(t, e1)
		dfy, e2 := m.FileLoad(f)
		assert.Nil(t, e2)
		cexp := dfx.Column(coln)
		// if sql, must pull data from query
		if which == "sql" {
			dfz, e3 := m.DBLoad(cexp.(*s.Col).MakeQuery(), dfx.Context().Dialect())
			assert.Nil(t, e3)
			cexp = dfz.Column(coln)
		}
		cact := dfy.Column(coln)
		assert.Equal(t, cexp.Data(), cact.Data())
	}
}

func TestParse_Table(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		out, e := dfx.Parse("table(y,yy)")
		assert.Nil(t, e)
		df1 := out.AsDF()
		e1 := df1.Sort(false, "count")
		assert.Nil(t, e1)
		col := df1.Column("count")
		assert.NotNil(t, col)
		assert.Equal(t, []int{2, 1, 1, 1, 1}, col.Data())

		_, e3 := dfx.Parse("table(x)")
		assert.NotNil(t, e3)
	}
}

func TestParse_Sort(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		_, e := dfx.Parse("sort('asc', y, x)")
		assert.Nil(t, e)
		//		assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, dfx.Column("y").Data())
		//	assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, dfx.Column("yy").Data())
	}
}

func TestWhere(t *testing.T) {
	for _, which := range pkgs() {
		// via methods
		fmt.Println(which)
		dfx := loadData(which)
		indCol, e := dfx.Parse("y==-5 || yy == 16")
		assert.Nil(t, e)
		indCol.AsColumn().Rename("ind")
		e1 := dfx.AppendColumn(indCol.AsColumn(), false)
		assert.Nil(t, e1)
		dfOut, e2 := dfx.Where(indCol.AsColumn())
		assert.Nil(t, e2)
		r := dfOut.Column("y")
		fmt.Println(r.Data())
		assert.Equal(t, []int{-5, 6}, dfOut.Column("y").Data())
		assert.Equal(t, []int{-15, 16}, dfOut.Column("yy").Data())

		// via Parse
		out, e3 := dfx.Parse("where(y == -5 || yy == 16)")
		assert.Nil(t, e3)
		assert.Equal(t, []int{-5, 6}, out.AsDF().Column("y").Data())
	}
}

func TestReplace(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		indCol, e0 := dfx.Parse("y==-5")
		assert.Nil(t, e0)
		indCol.AsColumn().Rename("ind")
		e := dfx.AppendColumn(indCol.AsColumn(), false)
		assert.Nil(t, e)
		coly := dfx.Column("y")
		assert.NotNil(t, coly)
		colyy := dfx.Column("yy")
		assert.NotNil(t, colyy)
		colR, e3 := coly.Replace(indCol.AsColumn(), colyy)
		assert.Nil(t, e3)
		assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, colR.Data()) // checker(dfx, "rep", which, colR, -1))

		// via Parse
		out, e4 := dfx.Parse("if(y==-5,yy,y)")
		assert.Nil(t, e4)
		assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, out.AsColumn().Data()) // checker(dfx, "rep", which, out.AsColumn(), -1))
	}
}

func TestParser(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		x := [][]any{
			{"x--3.0", 0, 4.0},
			{"sum(y)", 0, 12},
			{"sum(x)", 0, 7.5},
			{"dt != date(20221231)", 0, 0},
			{"dt != date(20221231)", 0, 0},
			{"dt != date(20221231)", 1, 1},
			{"dt == date(20221231)", 0, 1},
			{"dt == date(20221231)", 1, 0},
			{"4+1--1", 0, 6},
			{"if(y == 1, 2.0, (x))", 0, 2.0},
			{"if(y == 1, 2.0, (x))", 1, -2.0},
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
			//			{"string(float(1)+.234)", 0, "1.234"},
			{"float('1.1')", 0, 1.1},
			{"int(2.9)", 0, 2},
			{"float(1)", 0, 1.0},
			{"string(dt)", 0, "2022-12-31"},
			{"z!='20060102'", 0, 1},
			{"x--1.0", 0, 2.0},
			{"x*10.0", 0, 10.0},
			{"int(x)", 5, 3},
			{"(float(4+2) * abs(-3.0/2.0))", 0, 9.0},
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
			{"4+3", 0, 7},
			{"4-1-1-1-1", 0, 0},
			{"4+1-1", 0, 4},
			{"float(4)+1.0--1.0", 0, 6.0},
			{"exp(1.0)*abs(float(-2/(1+1)))", 0, math.Exp(1)},
			{"date( 20020630)", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
			{"date('2002-06-30')", 0, time.Date(2002, 6, 30, 0, 0, 0, 0, time.UTC)},
			{"((exp(1.0) + log(exp(1.0))))*(3.0--1.0)", 0, 4.0 + 4.0*math.Exp(1)},
			{"-x +2.0", 0, 1.0},
			{"-x +4.0", 1, 6.0},
			{"x/0.0", 0, math.Inf(1)},
			{"float((3.0 * 4.0 + 1.0 - -1.0)*(2.0 + abs(-1.0)))", 0, 42.0},
			{"(1 + 2) - -(-1 - 2)", 0, 0},
			{"(1.0 + 3.0) / abs(-(-1.0 + 3.0))", 0, 2.0},
		}

		cnt := 0
		for ind := 0; ind < len(x); ind++ {
			var r d.DF
			cnt++
			eqn := x[ind][0].(string)
			fmt.Println(eqn)
			xOut, e := dfx.Parse(eqn)
			assert.Nil(t, e)
			xOut.AsColumn().Rename("test")

			if which == "sql" {
				r, e = s.NewDFcol(nil, dfx.(*s.DF).Context(), xOut.AsColumn().(*s.Col))
			} else {
				r, e = m.NewDFcol(nil, dfx.(*m.DF).Context(), xOut.AsColumn().(*m.Col))
			}

			assert.Nil(t, e)
			result := checker(r, "test", which, nil, x[ind][1].(int))

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
	}
}

// TODO: consider dropping cat counts
func TestToCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		colx, e := dfx.Parse("date(z)")
		assert.Nil(t, e)
		col := colx.AsColumn()
		col.Rename("dt1")
		e = dfx.AppendColumn(col, false)
		assert.Nil(t, e)

		// try with DTint
		colx, e = dfx.Parse("cat(y)")
		assert.Nil(t, e)
		colx.AsColumn().Rename("test")
		result := checker(dfx, "test", which, colx.AsColumn(), -1)
		expected := []int{1, 0, 4, 1, 2, 3}
		assert.Equal(t, expected, result)

		// try with DTstring
		colx, e = dfx.Parse("cat(z)")
		assert.Nil(t, e)
		result = checker(dfx, "test", which, colx.AsColumn(), -1)
		expected = []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expected, result)

		// try with DTdate
		colx, e = dfx.Parse("cat(dt1)")
		assert.Nil(t, e)
		colx.AsColumn().Rename("test")
		result = checker(dfx, "test", which, colx.AsColumn(), -1)
		expected = []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expected, result)

		// try with fuzz > 1
		colx, e = dfx.Parse("cat(y, 2)")
		assert.Nil(t, e)
		result = checker(dfx, "test", which, colx.AsColumn(), -1)
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, result)

		// try with DTfloat
		_, e = dfx.Parse("cat(x)")
		assert.NotNil(t, e)
	}
}

func TestApplyCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		r, e := dfx.Parse("cat(y)")
		assert.Nil(t, e)
		sx := r.AsColumn()
		sx.Rename("caty")
		e1 := dfx.AppendColumn(sx, false)
		assert.Nil(t, e1)

		r2, e2 := dfx.Parse("applyCat(yy, caty, -5)")
		assert.Nil(t, e2)

		// -5 maps to 0 so all new values map to 0
		expected := []int{1, 0, 0, 1, 0, 0}
		assert.Equal(t, expected, r2.AsColumn().Data())

		// try with fuzz > 1
		r3, e3 := dfx.Parse("cat(y,2)")
		assert.Nil(t, e3)
		r3.AsColumn().Rename("caty2")
		e4 := dfx.AppendColumn(r3.AsColumn(), false)
		assert.Nil(t, e4)

		r5, e5 := dfx.Parse("applyCat(yy,caty2,-5)")
		assert.Nil(t, e5)
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, r5.AsColumn().Data())
	}
}

func TestAppendDF(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		dfy := loadData(which)
		dfOut, e := dfx.AppendDF(dfy)
		assert.Nil(t, e)
		exp := dfx.RowCount() + dfy.RowCount()
		assert.Equal(t, exp, dfOut.RowCount())
	}
}

func TestFilesOpen(t *testing.T) {
	dfx := loadData("mem")

	// specify both fieldNames and fieldTypes
	// file has no EOL characters
	fieldNames := []string{"k", "x", "y", "yy", "z", "dt"}
	fieldTypes := []d.DataTypes{d.DTint, d.DTfloat, d.DTint, d.DTint, d.DTstring, d.DTdate}
	fieldWidths := []int{1, 5, 2, 3, 10, 8}
	f := d.NewFiles()
	f.Strict, f.Header = false, false
	f.EOL = 0
	e := f.Open(d.Slash(os.Getenv("datapath"))+fileNameW1, fieldNames, fieldTypes, fieldWidths)
	assert.Nil(t, e)
	df1, e1 := m.FileLoad(f)
	assert.Nil(t, e1)
	for _, cn := range dfx.ColumnNames() {
		cx := dfx.Column(cn)
		assert.NotNil(t, cx)
		cy := df1.Column(cn)
		assert.NotNil(t, cy)
		assert.Equal(t, cx.Data(), cy.Data())
	}

	// file has EOL characters
	f = d.NewFiles()
	f.Strict, f.Header = false, false
	e4 := f.Open(d.Slash(os.Getenv("datapath"))+fileNameW2, fieldNames, fieldTypes, fieldWidths)
	assert.Nil(t, e4)
	df2, e5 := m.FileLoad(f)
	assert.Nil(t, e5)
	for _, cn := range dfx.ColumnNames() {
		cx := dfx.Column(cn)
		assert.NotNil(t, cx)
		cy := df2.Column(cn)
		assert.NotNil(t, cy)
		assert.Equal(t, cx.Data(), cy.Data())
	}

	// file has EOL characters and a header, but still specify these
	f = d.NewFiles()
	f.Strict, f.Header = false, true
	e8 := f.Open(d.Slash(os.Getenv("datapath"))+fileNameW3, fieldNames, fieldTypes, fieldWidths)
	assert.Nil(t, e8)
	df3, e9 := m.FileLoad(f)
	assert.Nil(t, e9)
	for _, cn := range dfx.ColumnNames() {
		cx := dfx.Column(cn)
		assert.NotNil(t, cx)
		cy := df3.Column(cn)
		assert.NotNil(t, cy)
		assert.Equal(t, cx.Data(), cy.Data())
	}

	// file has EOL characters and a header, have it read fieldNames and infer types
	f = d.NewFiles()
	f.Strict, f.Header = false, true
	e12 := f.Open(d.Slash(os.Getenv("datapath"))+fileNameW3, nil, nil, fieldWidths)
	assert.Nil(t, e12)
	df4, e13 := m.FileLoad(f)
	assert.Nil(t, e13)
	for _, cn := range dfx.ColumnNames() {
		cx := dfx.Column(cn)
		assert.NotNil(t, cx)
		cy := df4.Column(cn)
		assert.NotNil(t, cy)
		assert.Equal(t, cx.Data(), cy.Data())
	}
}

func TestFilesSave(t *testing.T) {
	dfx := loadData("men")
	fs := d.NewFiles()
	e0 := fs.Save(d.Slash(os.Getenv("datapath"))+fileName, dfx)
	assert.Nil(t, e0)

	f := d.NewFiles()
	f.Strict = false
	e := f.Open(d.Slash(os.Getenv("datapath"))+fileName, nil, nil, nil)
	assert.Nil(t, e)
	dfy, e1 := m.FileLoad(f)
	assert.Nil(t, e1)
	for _, cn := range dfx.ColumnNames() {
		cx := dfx.Column(cn)
		assert.NotNil(t, cx)
		cy := dfy.Column(cn)
		assert.NotNil(t, cy)
		assert.Equal(t, cx.Data(), cy.Data())
	}
}
