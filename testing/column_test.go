package testing

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	m "github.com/invertedv/df/mem"
	s "github.com/invertedv/df/sql"

	d "github.com/invertedv/df"
	"github.com/stretchr/testify/assert"
)

// tests for the parser
var (
	//go:embed tests.txt
	parserTests string
)

func TestCast(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		e := d.Parse(dfx, "test:=date('2020-12-31')")
		assert.Nil(t, e)
		dx := dfx.Column("test").Data().AsAny()
		_ = dx
		if col, ok := dfx.Column("test").(*s.Col); ok {
			q := col.MakeQuery()
			fmt.Println(q)
		}
		fmt.Println(dx)
	}
}

func TestTemp(t *testing.T) {
	dfx := loadData(pg + ",d1")
	outDF, e0 := dfx.By("y", "sx:=sum(x)", "count:=count(y)", "mx:=mean(global(x))", "mz:=mean(x)")
	assert.Nil(t, e0)
	d := outDF.Column("mx").Data().AsAny()
	fmt.Println(d)
}
func TestRandom(t *testing.T) {
	n := 100 //000000
	x := make([]float64, n)
	y := make([]float64, n)
	z := make([]float64, n)
	s := make([]string, n)
	//	dt := make([]time.Time, n)
	for ind := range n {
		x[ind] = float64(-ind + 1)
		y[ind] = float64(-ind + 1)
		z[ind] = float64(ind)
		s[ind] = fmt.Sprintf("%d", ind)
		//		s[ind] = fmt.Sprintf("%d", ind)
		//		l := ind % 40
		//		dt[ind] = time.Date(1980+l, 6, 1, 0, 0, 0, 0, time.UTC)

	}

	vx, _ := d.NewVector(x, d.DTfloat)
	vy, _ := d.NewVector(y, d.DTfloat)
	vz, _ := d.NewVector(z, d.DTfloat)
	vs, _ := d.NewVector(s, d.DTstring)
	//	vdt := d.NewVector(dt, 0)
	colx, ex := m.NewCol(vx, vx.VectorType(), d.ColName("x"))
	assert.Nil(t, ex)
	coly, ey := m.NewCol(vy, vy.VectorType(), d.ColName("y"))
	assert.Nil(t, ey)
	colz, ez := m.NewCol(vz, vz.VectorType(), d.ColName("z"))
	assert.Nil(t, ez)
	cols, es := m.NewCol(vs, vs.VectorType(), d.ColName("s"))
	assert.Nil(t, es)
	//	coldt, edt := m.NewCol(vdt, d.ColName("dt"))
	//	assert.Nil(t, edt)
	df, ed := m.NewDFcol(m.StandardFunctions(), []*m.Col{colx, coly, colz, cols})
	assert.Nil(t, ed)
	tx := time.Now()
	ep := d.Parse(df, "test:=x+y")
	//	outCol, ep := d.Parse(df, "dot(y,y)")
	assert.Nil(t, ep)

	fmt.Println("value ", df.Column("test").(*m.Col).Element(0))
	t1 := time.Since(tx).Seconds()
	fmt.Println(t1, " seconds")
	tx = time.Now()
	//	m := x[0]
	//	for ind := range < n {
	//		//z[ind] = x[ind] - y[ind] // x[ind] + y[ind]
	//		if x[ind] > m {
	//			m = x[ind]
	//		}
	//	}
	for ind := range n {
		//z[ind] = x[ind] - y[ind] // x[ind] + y[ind]
		z[ind] = x[ind] + y[ind] + y[ind]
	}

	t2 := time.Since(tx).Seconds()
	fmt.Println(t2, " seconds")
	fmt.Println(t1/t2, " ratio")
}

func TestRename(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		dfy := dfx.Copy()
		x := dfy.Column("x")
		xx := x.Parent()
		_ = xx
		e := dfx.Column("y").Rename("x")
		assert.NotNil(t, e)
		e = dfx.Column("y").Rename("aa")
		assert.Nil(t, e)
		x1 := dfx.Column("x").Data().AsAny()
		e = dfx.Column("x").Rename("xa")
		assert.Nil(t, e)
		e = dfx.Column("xa").Rename("xb")
		assert.Nil(t, e)
		assert.Equal(t, x1, dfx.Column("xb").Data().AsAny())
		e = dfx.Column("xb").Rename("x=")
		assert.NotNil(t, e)
	}
}

func TestRowNumber(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		e := d.Parse(dfx, "rn:=rowNumber()")
		assert.Nil(t, e)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, dfx.Column("rn").Data().AsAny())
	}
}

func TestIf(t *testing.T) {
	for _, which := range pkgs() {
		if !strings.Contains(which, "mem") {
			continue
		}
		dfx := loadData(which)
		e := d.Parse(dfx, "out:=if(y==-5,yy,y)")
		assert.Nil(t, e)
		assert.Equal(t, []int{1, -15, 6, 1, 4, 5},
			dfx.Column("out").Data().AsAny())

		e1 := d.Parse(dfx, "out:=if(z=='20221231',1.0,0.0)")
		assert.Nil(t, e1)
		assert.Equal(t, []float64{1, 0, 0, 0, 0, 0},
			dfx.Column("out").Data().AsAny())

		e2 := d.Parse(dfx, "out:=if(z=='20000101','a','b')")
		assert.Nil(t, e2)
		assert.Equal(t, []string{"b", "a", "b", "b", "b", "b"},
			dfx.Column("out").Data().AsAny())

		e3 := d.Parse(dfx, "out:=if(z=='20000101','a',if(y==1,'c','b'))")
		assert.Nil(t, e3)
		assert.Equal(t, []string{"c", "a", "b", "c", "b", "b"},
			dfx.Column("out").Data().AsAny())

		e4 := d.Parse(dfx, "out:=float(if(y==1 || yy==15,1,0))+1.0")
		assert.Nil(t, e4)
		assert.Equal(t, []float64{2, 1, 1, 2, 2, 1},
			dfx.Column("out").Data().AsAny())

		e5 := d.Parse(dfx, "out:=int(if(y==1,mean(yy),0.0)+1.0)")
		assert.Nil(t, e5)
		assert.Equal(t, []int{6, 1, 1, 6, 1, 1},
			dfx.Column("out").Data().AsAny())
	}
}

func TestParser(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		tests := strings.Split(parserTests, "\n")
		for _, test := range tests {
			fmt.Println(test, which)
			vals := strings.Split(strings.ReplaceAll(test, " ", ""), "|")
			if len(vals) != 4 {
				continue
			}
			e0 := d.Parse(dfx, "test:="+vals[0])
			assert.Nil(t, e0)
			ind, e1 := strconv.ParseInt(vals[1], 10, 64)
			assert.Nil(t, e1)
			var result any
			switch vals[2] {
			case "float":
				var e2 error
				result, e2 = strconv.ParseFloat(vals[3], 64)
				assert.Nil(t, e2)
			case "int":
				var e3 error
				result, e3 = strconv.ParseInt(vals[3], 10, 64)
				result = int(result.(int64))
				assert.Nil(t, e3)
			case "string":
				result = vals[3]
			case "date":
				var e4 error
				result, e4 = time.Parse("2006-01-02", vals[3])
				assert.Nil(t, e4)
			}
			actual := dfx.Column("test").Data().Element(int(ind))

			if vals[2] != "float" {
				assert.Equal(t, result, actual)
				continue
			}

			assert.InEpsilon(t, result.(float64), actual.(float64), 0.001)
		}
	}
}

//			{"quantile(y,1.0)", 0, 6.0},
//			{"quantile(y,0.0)", 0, -5.0},
//			{"max(dt)", 0, time.Date(2023, 9, 15, 0, 0, 0, 0, time.UTC)},
//			{"min(dt)", 0, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
//			{"min(z)", 0, "20000101"},
//			{"max(z)", 0, "20230915"},
//			{"min(y)", 0, -5},
//			{"max(y)", 0, 6},
//			{"min(x)", 0, -2.0},
//			{"max(x)", 0, 3.5},
//			{"quantile(y,0.5)", 0, 1.0},
//			{"quantile(x,0.5)", 0, 1.0},
//			{"median(y)", 0, 1.0},
//			{"median(x)", 0, 1.0},
//			{"sdev(y)", 0, 4.0},
//			{"sdev(x)", 0, 2.043},
//			{"var(y)", 0, 16.0},
//			{"var(x)", 0, 4.175},

// TODO: check consistency of paradigm of defining with var() before ifs
// TODO: check for referencing elements directly not through method
// TODO: in mem there's some weirdness in NewCol if data is a Vector -- don't need datatype then
// TODO: revisit sourceDF relative to mem/By
// TODO: think about mem.NewCol when feeding a Vector

func TestToCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		e6 := d.Parse(dfx, "fuzz := 2")
		assert.Nil(t, e6)
		e0 := d.Parse(dfx, "dt1:=date(z)")
		assert.Nil(t, e0)

		// try with DTdate
		e3x := d.Parse(dfx, "test:=cat(dt1)")
		assert.Nil(t, e3x)
		resultx := dfx.Column("test").Data().AsAny()
		expectedx := []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expectedx, resultx)

		// try with DTfloat
		e5 := d.Parse(dfx, "cat(x)")
		assert.NotNil(t, e5)

		fmt.Println(dfx.ColumnNames())
		e1 := d.Parse(dfx, "test:=cat(y)")
		fmt.Println(dfx.ColumnNames())
		assert.Nil(t, e1)
		result := dfx.Column("test").Data().AsAny()
		expected := []int{1, 0, 4, 1, 2, 3}
		assert.Equal(t, expected, result)

		// try with DTstring
		e2 := d.Parse(dfx, "test:=cat(z)")
		assert.Nil(t, e2)
		result = dfx.Column("test").Data().AsAny()
		expected = []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expected, result)

		// try with fuzz > 1
		e4 := d.Parse(dfx, "test:=cat(y, fuzz)")
		assert.Nil(t, e4)
		result = dfx.Column("test").Data().AsAny()
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, result)

		// try with DTdate
		//		_, e3 := d.Parse(dfx, "test:=cat(dt1)")
		//		assert.Nil(t, e3)
		//		result = dfx.Column("test").Data().AsAny()
		//		expected = []int{3, 0, 1, 1, 4, 2}
		//		assert.Equal(t, expected, result)

	}
}

func TestApplyCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		e := d.Parse(dfx, "caty:=cat(y)")
		assert.Nil(t, e)

		// try with a default that isn't in original data
		e6 := d.Parse(dfx, "test1:=applyCat(yy,caty,100)")
		assert.NotNil(t, e6)

		e2 := d.Parse(dfx, "test1:=applyCat(yy, caty, -5)")
		assert.Nil(t, e2)

		// -5 maps to 0 so all new values map to 0
		expected := []int{1, 0, 0, 1, 0, 0}
		assert.Equal(t, expected, dfx.Column("test1").Data().AsAny())

		// try with fuzz > 1
		e3 := d.Parse(dfx, "caty2:=cat(y,2)")
		assert.Nil(t, e3)

		e5 := d.Parse(dfx, "test2:=applyCat(yy,caty2,-5)")
		assert.Nil(t, e5)
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, dfx.Column("test2").Data().AsAny())
	}
}

// TODO: think about float -> string
// TODO: think about date formats
