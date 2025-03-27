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

func TestStringer(t *testing.T) {
	for _, which := range pkgs("d1") {
		if !strings.Contains(which, "click") {
			continue
		}
		dfx := loadData(which)
		e := d.Parse(dfx, "cat := cat(y)")
		assert.Nil(t, e)
		fmt.Println(dfx)
	}
}

// TODO: call to NewDFseq not the same between mem and sql
func TestInterp(t *testing.T) {
	for _, which := range pkgs("d1") {
		//		if !strings.Contains(which, "click") {
		//			continue
		//		}

		dfx := loadData(which)
		var (
			dfI d.DF
			e   error
		)
		if strings.Contains(which, "mem") {
			dfI, e = m.NewDFseq(nil, 15)

		} else {
			dfI, e = s.NewDFseq(nil, dfx.Dialect(), 15)
		}
		assert.Nil(t, e)

		e = d.Parse(dfx, "kk := float(y)")
		assert.Nil(t, e)
		e = d.Parse(dfI, "kx := float(seq) ")
		assert.Nil(t, e)

		dfOut, e1 := dfx.Interp(dfI, "kk", "kx", "x", "xhat")
		assert.Nil(t, e1)
		fmt.Println(which)
		fmt.Println(dfOut.ColumnNames())
		fmt.Println(dfOut.Column("seq").Data().AsAny())
		fmt.Println(dfOut.Column("xhat").Data().AsAny())
		exp := []float64{0.08333333333333337, 0.5, 1, 1.5, 2, 3.5, 3}
		act := dfOut.Column("xhat").Data().AsAny().([]float64)
		for ind, xexp := range exp {
			assert.InEpsilon(t, xexp, act[ind], .00001)
		}
	}
}

func TestNull(t *testing.T) {
	defInt := 100
	dfy := loadFile("d2.csv", d.FileDefaultInt(defInt))
	dx := dfy.Column("k").Data().AsAny()
	assert.Equal(t, defInt, dx.([]int)[2])

	dfx := loadData("postgres,d2")
	_ = d.DialectDefaultInt(defInt)(dfx.Dialect())
	dx = dfx.Column("k").Data().AsAny()
	assert.Equal(t, defInt, dx.([]int)[2])
	_ = dx
	fmt.Println(dx)
}

func TestString(t *testing.T) {
	var sBase string
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		if sBase == "" {
			sBase = dfx.Column("x").String()
			continue
		}
		s := dfx.Column("x").String()
		assert.Equal(t, sBase, s)
		fmt.Println(which)
		fmt.Println(dfx.Column("R"))
	}
}

func TestSeq(t *testing.T) {
	for _, which := range pkgs("d1") {
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

	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		dlct := dfx.Dialect()
		src := strings.Split(which, ",")[0]

		// save to a table
		var outTable string
		var options []string
		switch src {
		case ch:
			outTable = os.Getenv("chTemp")
		case pg:
			outTable = os.Getenv("pgTemp")
			options = []string{"Owner:" + owner, "TableSpace:" + tablespace}
		case mem:
			outTable = os.Getenv("pgTemp")
			options = []string{"Owner:" + owner, "TableSpace:" + tablespace}
		}

		// test entire data frame
		e := dlct.Save(outTable, "k,yy", true, dfx, options...)
		assert.Nil(t, e)
		dfy, ex := m.DBLoad("SELECT * FROM "+outTable, dfx.Dialect())
		assert.Nil(t, ex)
		assert.Equal(t, dfy.Column(coln).Data().AsAny(), dfx.Column(coln).Data().AsAny())

		// test vector
		_ = dfx.Sort(true, coln)
		vec := dfx.Column(coln).Data()
		e = dlct.Save(outTable, "", true, vec, options...)
		assert.Nil(t, e)
		dfy, ex = m.DBLoad("SELECT * FROM "+outTable, dfx.Dialect())
		assert.Nil(t, ex)
		assert.Equal(t, dfy.Column("col1").Data().AsAny(), dfx.Column(coln).Data().AsAny())

		// test column
		col := dfx.Column(coln)
		e = dlct.Save(outTable, "", true, col, options...)
		assert.Nil(t, e)
		dfy, ex = m.DBLoad("SELECT * FROM "+outTable, dfx.Dialect())
		assert.Nil(t, ex)
		assert.Equal(t, dfy.Column(coln).Data().AsAny(), dfx.Column(coln).Data().AsAny())

		fmt.Println("yay!")

	}
}

func TestFileSave(t *testing.T) {
	const coln = "b"

	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e0 := d.Parse(dfx, "a := 2*y")
		assert.Nil(t, e0)
		e3 := d.Parse(dfx, "b := 2*a")
		assert.Nil(t, e3)
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
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e := d.Parse(dfx, "y1:=2*k")
		assert.Nil(t, e)
		dfy := dfx.Copy()
		_ = dfy.Column("x").Rename("xx")
		_ = dfy.Column("z").Rename("zz")

		outDF, e := dfx.Join(dfy, "y1")
		assert.Nil(t, e)
		fmt.Println(outDF.ColumnNames())
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
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		outDF, e0 := dfx.By("y", "sx:=sum(x)", "count:=count(y)", "mx:=mean(global(x))", "mz:=mean(x)")
		assert.Nil(t, e0)
		e1 := outDF.Sort(false, "count")
		assert.Nil(t, e1)
		assert.Equal(t, []int{2, 1, 1, 1, 1}, outDF.Column("count").Data().AsAny())
		assert.Equal(t, []float64{1.25, 1.25, 1.25, 1.25, 1.25}, outDF.Column("mx").Data().AsAny())
		col, _ := outDF.Column("sx").Data().AsFloat()
		sort.Float64s(col)
		assert.Equal(t, []float64{-2, 1, 2, 3, 3.5}, col)
	}
}

func TestParse_Table(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		outDF, e := dfx.Table("y", "yy")
		assert.Nil(t, e)

		//cx := dfx.Column("x")
		//		_ = d.ColName("xx")(cx)
		//		ez := df1.AppendColumn(cx, true)
		//		assert.NotNil(t, ez)

		fmt.Println(outDF.Column("rate").Data().AsAny())

		e1 := outDF.Sort(false, "count")
		assert.Nil(t, e1)
		col := outDF.Column("count").Data().AsAny()
		assert.NotNil(t, col)
		assert.Equal(t, []int{2, 1, 1, 1, 1}, col)
	}
}

func TestParse_Sort(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e := dfx.Sort(true, "y", "x")
		assert.Nil(t, e)

		assert.Equal(t, []int{-5, 1, 1, 4, 5, 6}, dfx.Column("y").Data().AsAny())
		assert.Equal(t, []int{-15, 1, 1, 15, 14, 16}, dfx.Column("yy").Data().AsAny())

		e1 := dfx.Sort(false, "y", "x")
		assert.Nil(t, e1)
		fmt.Println(dfx.Column("y").Data().AsAny())
		assert.Equal(t, []int{6, 5, 4, 1, 1, -5}, dfx.Column("y").Data().AsAny())
	}
}

func TestWhere(t *testing.T) {
	for _, which := range pkgs("d1") {
		// via methods
		fmt.Println(which)
		dfx := loadData(which)
		dfOut, e := dfx.Where("y==-5 || yy == 16")
		assert.Nil(t, e)
		r := dfOut.Column("y")
		fmt.Println(r.Data())
		assert.Equal(t, []int{-5, 6}, dfOut.Column("y").Data().AsAny())
		assert.Equal(t, []int{-15, 16}, dfOut.Column("yy").Data().AsAny())
	}
}

func TestAppendDF(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e := d.Parse(dfx, "test :=k")
		fmt.Println(dfx.ColumnNames())
		assert.Nil(t, e)
		dfy := loadData(which)
		_, e1 := dfx.AppendDF(dfy)
		assert.NotNil(t, e1)
		e2 := d.Parse(dfy, "test :=2*k")
		assert.Nil(t, e2)

		dfOut, e3 := dfx.AppendDF(dfy)
		dfOut.Sort(false, "test")
		assert.Nil(t, e3)
		exp := dfx.RowCount() + dfy.RowCount()
		assert.Equal(t, exp, dfOut.RowCount())
		assert.Equal(t,
			[]int{12, 10, 8, 6, 6, 5, 4, 4, 3, 2, 2, 1},
			dfOut.Column("test").Data().AsAny())
	}
}

func TestFilesOpen(t *testing.T) {
	dfx := loadData(mem + ",d1")
	// other files don't have column R
	e := dfx.DropColumns("R")
	assert.Nil(t, e)

	// specify both fieldNames and fieldTypes
	// file has no eol characters
	fieldNames := []string{"k", "x", "y", "yy", "z", "dt"}
	fieldTypes := []d.DataTypes{d.DTint, d.DTfloat, d.DTint, d.DTint, d.DTstring, d.DTdate}
	fieldWidths := []int{1, 5, 2, 3, 10, 8}
	f, _ := d.NewFiles(d.FileEOL(0), d.FileHeader(false), d.FileStrict(false),
		d.FileFieldNames(fieldNames), d.FileFieldTypes(fieldTypes), d.FileFieldWidths(fieldWidths))
	e = f.Open(slash(os.Getenv("datapath")) + fileNameW1)
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
