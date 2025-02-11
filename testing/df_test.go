package testing

import (
	"fmt"
	s "github.com/invertedv/df/sql"
	"os"
	"testing"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	sql := "SELECT (SELECT count(*) FROM testing.d1) AS r"
	db := newConnectCH("", "root", "abc234")
	dlct, e := d.NewDialect(ch, db)
	assert.Nil(t, e)

	qry := "select * from testing.d1"
	_ = qry
	v, _, _, e3 := dlct.Load(sql)
	assert.Nil(t, e3)
	_ = v
	for ind := 0; ind < len(v); ind++ {
		fmt.Println(v[ind].Data().AsAny())
	}
	return
	rows, e2 := db.Query("select z, k from testing.d1 limit 1")
	ct, e4 := rows.ColumnTypes()
	assert.Nil(t, e4)
	var ry []any
	for ind := 0; ind < len(ct); ind++ {
		var x any
		ry = append(ry, &x)
	}
	for rows.Next() {
		e5 := rows.Scan(ry...)
		assert.Nil(t, e5)
	}

	assert.Nil(t, e2)
	var x any
	var y *uint64
	_, _ = x, y
	for rows.Next() {
		ex := rows.Scan(&y)
		assert.Nil(t, ex)
		switch x := x.(type) {
		case uint64:
			fmt.Println(x, "uint")
		case *uint64:
			fmt.Println(*x, "*uint64")
		}
	}
	df, e1 := m.DBLoad(sql, dlct)
	assert.Nil(t, e1)
	_ = df
}

// slash adds a trailing slash if inStr doesn't end in a slash
func slash(inStr string) string {
	if inStr[len(inStr)-1] == '/' {
		return inStr
	}

	return inStr + "/"
}

func TestPlotXY(t *testing.T) {
	dfx := loadData("mem")
	e := dfx.Sort(true, "x")
	assert.Nil(t, e)
	p, e0 := d.NewPlot(d.PlotTitle("This Is A Test"), d.PlotXlabel("X-Axis"),
		d.PlotYlabel("Y-Axis"), d.PlotLegend(true))
	assert.Nil(t, e0)
	_ = d.PlotSubtitle("(subtitle here)")(p)
	_ = d.PlotXlabel("New X Label")(p)
	_ = d.PlotTitle("What???")(p)
	_ = d.PlotHeight(800)(p)
	_ = d.PlotWidth(800)(p)
	x := dfx.Column("x")
	y, _ := d.Parse(dfx, "exp(x)")
	_ = d.ColName("expy")(y.Column())
	e1 := p.PlotXY(x.Data().AsAny().([]float64), y.Column().Data().AsAny().([]float64), "s1", "red")
	assert.Nil(t, e1)
	//	e2 := p.PlotXY(x, x, "s2", "black")
	//	assert.Nil(t, e2)
	//	e3 := p.Show("", "")
	//	assert.Nil(t, e3)

	e4 := d.PlotHeight(10)(p)
	assert.NotNil(t, e4)
}

/*func TestTypes(t *testing.T) {
	table := "SELECT * EXCEPT(fhfa_msad, delta) FROM fhfa.msad LIMIT 10"
	var db *sql.DB

	user := os.Getenv("user")
	host := os.Getenv("host")
	password := os.Getenv("password")
	db = newConnectCH(host, user, password)

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
}*/

func TestString(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		fmt.Println(dfx)
	}
}

/*
	func TestSeq(t *testing.T) {
		for _, which := range pkgs() {
			dfx := loadData(which)
			var df d.DF
			switch which {
			case mem:
				df = m.NewDFseq(nil, nil, 5)
			default:
				df = s.NewDFseq(nil, dfx.Context(), 5)
			}

			col := df.Column("seq")
			assert.NotNil(t, col)
			assert.Equal(t, []int{0, 1, 2, 3, 4}, col.Data())
		}
	}
*/
func TestSQLsave(t *testing.T) {
	const coln = "x"
	owner := os.Getenv("user")
	tablespace := os.Getenv("tablespace")

	for _, which := range pkgs() {
		dfx := loadData(which)
		dlct := dfx.Dialect()

		// save to a table
		var outTable string
		var options []string
		switch which {
		case ch:
			outTable = outTableCH
		case pg:
			outTable = outTablePG
			options = []string{"?Owner:" + owner, "?TableSpace:" + tablespace}
		case mem:
			outTable = outTablePG
			options = []string{"?Owner:" + owner, "?TableSpace:" + tablespace}
		}
		e := dlct.Save(outTable, "k,yy", true, dfx, options...)
		assert.Nil(t, e)
		dfy, ex := m.DBLoad("SELECT * FROM "+outTable, dfx.Dialect())
		assert.Nil(t, ex)
		assert.Equal(t, dfy.Column(coln).Data().AsAny(), dfx.Column(coln).Data().AsAny())
	}
}

func TestFileSave(t *testing.T) {
	const coln = "x"

	for _, which := range pkgs() {
		dfx := loadData(which)
		f1, _ := d.NewFiles()

		fn := slash(os.Getenv("datapath")) + fileName
		e := f1.Save(fn, dfx)
		assert.Nil(t, e)

		ct, _ := dfx.ColumnTypes()
		f, _ := d.NewFiles(d.FileFieldNames(dfx.ColumnNames()), d.FileFieldTypes(ct))
		e1 := f.Open(fn)
		assert.Nil(t, e1)
		dfy, e2 := m.FileLoad(f)
		assert.Nil(t, e2)
		cexp := dfx.Column(coln)
		// if sql, must pull data from query
		/*		if which != mem {
				dfz, e3 := m.DBLoad(cexp.(*s.Col).MakeQuery(), dfx.Context().Dialect())
				assert.Nil(t, e3)
				cexp = dfz.Column(coln)
			}*/
		cact := dfy.Column(coln)
		assert.Equal(t, cexp.Data(), cact.Data())
	}
}

func TestParse_By(t *testing.T) {
	for _, which := range pkgs() {
		if which == mem {
			continue
		}

		dfx := loadData(which).(*s.DF)

		_, e2 := d.Parse(dfx, "a:=mean(x)")
		assert.Nil(t, e2)
		fmt.Println(dfx.Column("a").Data().AsAny())

		dfy, e := dfx.By("y", "n:=count()", "r:=sum(x)")
		assert.Nil(t, e)

		_, e = d.Parse(dfy, "zx:=mean(x)")
		assert.Nil(t, e)
		_, e = d.Parse(dfy, "zt := global(sum(y))")

		fmt.Println(dfy.Column("n").Data().AsAny())
		fmt.Println(dfy.Column("zx").Data().AsAny())
		fmt.Println(dfy.Column("zt").Data().AsAny())

	}

}

func TestParse_Table(t *testing.T) {
	for _, which := range pkgs() {
		if which != pg {
			continue
		}
		dfx := loadData(which)
		out, e := d.Parse(dfx, "table(y,yy)")
		assert.Nil(t, e)
		df1 := out.DF()

		cx := dfx.Column("x")
		//		_ = d.ColParent(nil)(cx)
		d.ColName("xx")(cx)
		ez := df1.AppendColumn(cx, true)
		assert.NotNil(t, ez)

		q := df1.Column("rate").(*s.Col).MakeQuery()
		_ = q
		fmt.Println(df1.Column("rate").Data().AsAny())

		e1 := df1.Sort(false, "count")
		assert.Nil(t, e1)
		col := df1.Column("count").Data().AsAny()
		assert.NotNil(t, col)
		assert.Equal(t, []int{2, 1, 1, 1, 1}, col)

		_, e3 := d.Parse(dfx, "table(x)")
		assert.NotNil(t, e3)
	}
}

func TestParse_Sort(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		outdf, e := d.Parse(dfx, "sort('asc', y, x)")
		_ = outdf
		assert.Nil(t, e)
		assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, inter(dfx.Column("y")))
		assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, inter(dfx.Column("yy")))
	}
}

func TestWhere(t *testing.T) {
	for _, which := range pkgs() {
		// via methods
		fmt.Println(which)
		dfx := loadData(which)
		indCol, e := d.Parse(dfx, "y==-5 || yy == 16")
		assert.Nil(t, e)
		e0 := d.ColName("ind")(indCol.Column())
		assert.Nil(t, e0)
		e1 := dfx.Core().AppendColumn(indCol.Column(), false)
		assert.Nil(t, e1)
		dfOut, e2 := dfx.Where(indCol.Column())
		assert.Nil(t, e2)
		r := dfOut.Column("y")
		fmt.Println(r.Data())
		assert.Equal(t, []int{-5, 6}, inter(dfOut.Column("y")))
		assert.Equal(t, []int{-15, 16}, inter(dfOut.Column("yy")))

		// via Parse
		out, e3 := d.Parse(dfx, "where(y == -5 || yy == 16)")
		assert.Nil(t, e3)
		assert.Equal(t, []int{-5, 6}, inter(out.DF().Column("y")))
	}
}

func TestAppendDF(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		dfy := loadData(which)
		dfOut, e := dfx.AppendDF(dfy)
		q := dfOut.MakeQuery()
		_ = q
		assert.Nil(t, e)
		exp := dfx.RowCount() + dfy.RowCount()
		assert.Equal(t, exp, dfOut.RowCount())
	}
}

func TestFilesOpen(t *testing.T) {
	dfx := loadData(mem)

	// specify both fieldNames and fieldTypes
	// file has no eol characters
	fieldNames := []string{"k", "x", "y", "yy", "z", "dt"}
	fieldTypes := []d.DataTypes{d.DTint, d.DTfloat, d.DTint, d.DTint, d.DTstring, d.DTdate}
	fieldWidths := []int{1, 5, 2, 3, 10, 8}
	f, _ := d.NewFiles(d.FileEOL(0), d.FileHeader(false), d.FileStrict(false),
		d.FileFieldNames(fieldNames), d.FileFieldTypes(fieldTypes), d.FileFieldWidths(fieldWidths))
	e := f.Open(slash(os.Getenv("datapath")) + fileNameW1)
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

	// file has eol characters
	f, _ = d.NewFiles(d.FileHeader(false), d.FileStrict(false),
		d.FileFieldNames(fieldNames), d.FileFieldTypes(fieldTypes), d.FileFieldWidths(fieldWidths))
	e4 := f.Open(slash(os.Getenv("datapath")) + fileNameW2)
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

	// file has eol characters and a header, but still specify these
	f, _ = d.NewFiles(d.FileHeader(true), d.FileStrict(false),
		d.FileFieldNames(fieldNames), d.FileFieldTypes(fieldTypes), d.FileFieldWidths(fieldWidths))
	e8 := f.Open(slash(os.Getenv("datapath")) + fileNameW3)
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

	// file has eol characters and a header, have it read fieldNames and infer types
	f, _ = d.NewFiles(d.FileHeader(true), d.FileStrict(false), d.FileFieldWidths(fieldWidths))
	e12 := f.Open(slash(os.Getenv("datapath")) + fileNameW3)
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
	dfx := loadData(mem)
	fs, _ := d.NewFiles()
	e0 := fs.Save(slash(os.Getenv("datapath"))+fileName, dfx)
	assert.Nil(t, e0)

	f, _ := d.NewFiles(d.FileStrict(false))
	e := f.Open(slash(os.Getenv("datapath")) + fileName)
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
