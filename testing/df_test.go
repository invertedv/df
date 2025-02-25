package testing

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
	s "github.com/invertedv/df/sql"
	"github.com/stretchr/testify/assert"
)

func TestPlotXY(t *testing.T) {
	dfx := loadData("mem,d1")
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

func TestString(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		fmt.Println(dfx)
	}
}

func TestSeq(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		var (
			df d.DF
			e  error
		)

		switch which {
		case mem:
			df, e = m.NewDFseq(nil, 5)
			assert.Nil(t, e)
		default:
			df, e = s.NewDFseq(nil, dfx.Dialect(), 5)
			assert.Nil(t, e)
		}

		col := df.Column("seq")
		assert.NotNil(t, col)
		assert.Equal(t, []int{0, 1, 2, 3, 4}, col.Data().AsAny())
	}
}

func TestSQLsave(t *testing.T) {
	const coln = "x"
	owner := os.Getenv("user")
	tablespace := os.Getenv("tablespace")

	for _, which := range pkgs() {
		dfx := loadData(which)
		dlct := dfx.Dialect()
		src := strings.Split(which, ",")[0]

		// save to a table
		var outTable string
		var options []string
		switch src {
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
		cact := dfy.Column(coln)
		assert.Equal(t, cexp.Data(), cact.Data())
	}
}

func TestParse_Join(t *testing.T) {
	for _, which := range pkgs() {
		//		if !strings.Contains(which, "postgres") {
		//			continue
		//		}
		dfx := loadData(which)
		dfy := dfx.Copy()
		_ = dfy.Column("x").Rename("xx")
		_ = dfy.Column("z").Rename("zz")

		outDF, e := dfx.Join(dfy, "k")
		assert.Nil(t, e)
		fmt.Println(outDF.ColumnNames())
		//		q := outDF.(*s.DF).MakeQuery()
		//		_ = q
		fmt.Println(outDF.Column("yDUP").Data().AsAny())
		fmt.Println(outDF.Column("x").Data().AsAny())
		fmt.Println(outDF.Column("xx").Data().AsAny())
		assert.Equal(t, outDF.Column("xx").Data().AsAny(),
			outDF.Column("x").Data().AsAny())
		assert.Equal(t, outDF.Column("y").Data().AsAny(),
			outDF.Column("yDUP").Data().AsAny())

	}
}

func TestParse_By(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		outDF, e0 := d.Parse(dfx, "by(y,'sx:=sum(x)','count:=count(y)', 'mx:=mean(global(x))', 'mz:=mean(x)')")
		assert.Nil(t, e0)
		df := outDF.DF()
		e1 := df.Sort(false, "count")
		assert.Nil(t, e1)
		assert.Equal(t, []int{2, 1, 1, 1, 1}, df.Column("count").Data().AsAny())
		assert.Equal(t, []float64{1.25, 1.25, 1.25, 1.25, 1.25}, df.Column("mx").Data().AsAny())
		col, _ := df.Column("sx").Data().AsFloat()
		sort.Float64s(col)
		assert.Equal(t, []float64{-2, 1, 2, 3, 3.5}, col)
	}
}

func TestParse_Table(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		out, e := d.Parse(dfx, "table(y,yy)")
		assert.Nil(t, e)
		df1 := out.DF()

		cx := dfx.Column("x")
		_ = d.ColName("xx")(cx)
		ez := df1.AppendColumn(cx, true)
		assert.NotNil(t, ez)

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
		assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, dfx.Column("y").Data().AsAny())
		assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, dfx.Column("yy").Data().AsAny())
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
		assert.Equal(t, []int{-5, 6}, dfOut.Column("y").Data().AsAny())
		assert.Equal(t, []int{-15, 16}, dfOut.Column("yy").Data().AsAny())

		// via Parse
		out, e3 := d.Parse(dfx, "where(y == -5 || yy == 16)")
		assert.Nil(t, e3)
		assert.Equal(t, []int{-5, 6}, out.DF().Column("y").Data().AsAny())
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
	dfx := loadData(mem + ",d1")

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
	dfx := loadData("mem,d1")
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
