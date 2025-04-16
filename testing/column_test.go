package testing

import (
	_ "embed"
	"fmt"
	"math"
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

func TestStuff(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e := d.Parse(dfx, "xNorm := abs(x) / global(sum(abs(x)))")
		assert.Nil(t, e)
		fmt.Println(dfx.Column("xNorm").Data().AsAny())
		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestBinRandomGen(t *testing.T) {
	/*
		3rd argument:
		  - mem can be anything, even a constant
		  - clickhouse any numeric column
		  - postgres any numeric column that's in the original table
	*/
	const (
		nRep = 400000
		n    = 100
		p    = 0.25
	)

	for _, which := range pkgs("d1") {
		// slow on postgres so skip
		if strings.Contains(which, "post") {
			continue
		}

		dfx := loadData(which)
		var (
			dfy d.DF
			e   error
		)

		if strings.Contains(which, "mem") {
			dfy, e = m.NewDFseq(nRep)
		} else {
			dfy, e = s.NewDFseq(dfx.Dialect(), nRep)
		}
		assert.Nil(t, e)

		e = d.Parse(dfy, fmt.Sprintf("u := randBin(%d,%4.2f,seq)", n, p))
		assert.Nil(t, e)

		dfz, e1 := dfy.By("", "m:=mean(u)", "c := count(seq)", "v :=var(u)")
		assert.Nil(t, e1)
		m := dfz.Column("m").Data().Element(0).(float64)
		v := dfz.Column("v").Data().Element(0).(float64)
		c := dfz.Column("c").Data().Element(0).(int)
		fmt.Println(c, m, v)
		assert.Equal(t, nRep, c)
		assert.InEpsilon(t, n*p*(1-p), v, 0.01)
		assert.InEpsilon(t, n*p, m, 0.01)

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestBernRandomGen(t *testing.T) {
	const (
		nRep = 300000
		p    = 0.25
	)

	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		var (
			dfy d.DF
			e   error
		)

		if strings.Contains(which, "mem") {
			dfy, e = m.NewDFseq(nRep)
		} else {
			dfy, e = s.NewDFseq(dfx.Dialect(), nRep)
		}
		assert.Nil(t, e)

		e = d.Parse(dfy, fmt.Sprintf("u := randBern(%4.2f,seq)", p))
		assert.Nil(t, e)

		dfz, e1 := dfy.By("", "m:=mean(u)", "c := count(seq)", "v :=var(u)")
		assert.Nil(t, e1)
		m := dfz.Column("m").Data().Element(0).(float64)
		v := dfz.Column("v").Data().Element(0).(float64)
		c := dfz.Column("c").Data().Element(0).(int)
		fmt.Println(c, m, v)
		assert.Equal(t, nRep, c)
		assert.InEpsilon(t, p*(1-p), v, 0.01)
		assert.InEpsilon(t, p, m, 0.01)

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestExpRandomGen(t *testing.T) {
	const (
		nRep = 600000
		lambda = 3.0
	)

	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		var (
			dfy d.DF
			e   error
		)

		if strings.Contains(which, "mem") {
			dfy, e = m.NewDFseq(nRep)
		} else {
			dfy, e = s.NewDFseq(dfx.Dialect(), nRep)
		}
		assert.Nil(t, e)

		e = d.Parse(dfy, fmt.Sprintf("u := randExp(%4.2f,seq)", lambda))
		assert.Nil(t, e)

		dfz, e1 := dfy.By("", "m:=mean(u)", "c := count(seq)", "v :=var(u)")
		assert.Nil(t, e1)
		m := dfz.Column("m").Data().Element(0).(float64)
		v := dfz.Column("v").Data().Element(0).(float64)
		c := dfz.Column("c").Data().Element(0).(int)
		fmt.Println(c, m, v)
		assert.Equal(t, nRep, c)
		assert.InEpsilon(t, (1/lambda)*(1/lambda), v, 0.01)
		assert.InEpsilon(t, 1/lambda, m, 0.01)

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestUnifRandomGen(t *testing.T) {
	const n = 100000
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		var (
			dfy d.DF
			e   error
		)

		if strings.Contains(which, "mem") {
			dfy, e = m.NewDFseq(n)
		} else {
			dfy, e = s.NewDFseq(dfx.Dialect(), n)
		}
		assert.Nil(t, e)

		e = d.Parse(dfy, "u := randUnif(seq)")
		assert.Nil(t, e)

		dfz, e1 := dfy.By("", "m:=mean(u)", "c := count(seq)", "s :=std(u)")
		assert.Nil(t, e1)
		m := dfz.Column("m").Data().Element(0).(float64)
		s := dfz.Column("s").Data().Element(0).(float64)
		c := dfz.Column("c").Data().Element(0).(int)
		assert.Equal(t, c, n)
		assert.InEpsilon(t, math.Sqrt(1.0/12.0), s, 0.01)
		assert.InEpsilon(t, 0.5, m, 0.01)

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestNormRandomGen(t *testing.T) {
	const n = 100000
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		var (
			dfy d.DF
			e   error
		)

		if strings.Contains(which, "mem") {
			dfy, e = m.NewDFseq(n)
		} else {
			dfy, e = s.NewDFseq(dfx.Dialect(), n)
		}
		assert.Nil(t, e)
		e = d.Parse(dfy, "k := int(seq/10)")
		assert.Nil(t, e)
		e = d.Parse(dfy, "z := randNorm(seq)")
		assert.Nil(t, e)
		dfz, e1 := dfy.By("", "m:=mean(z)", "c := count(seq)", "s :=std(z)")
		assert.Nil(t, e1)
		m := dfz.Column("m").Data().Element(0).(float64)
		s := dfz.Column("s").Data().Element(0).(float64)
		c := dfz.Column("c").Data().Element(0).(int)
		assert.Equal(t, c, n)
		assert.InEpsilon(t, 1.0, s, 0.01)
		assert.InEpsilon(t, 1.0, m+1, 0.01)

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestColumnwise(t *testing.T) {
	for _, which := range pkgs("d1") {
		//		if !strings.Contains(which, "post") {
		//			continue
		//		}
		dfx := loadData(which)

		ex := d.Parse(dfx, "avg := colStd(x,2.0*x,-2.0*x)")
		assert.Nil(t, ex)
		fmt.Println(dfx.Column("avg").Data().AsAny())

		e := d.Parse(dfx, "s := 2.0*x")
		assert.Nil(t, e)

		e = d.Parse(dfx, "v := '20040101'")
		assert.Nil(t, e)

		e = d.Parse(dfx, "d := date('20040101')")
		assert.Nil(t, e)

		e = d.Parse(dfx, "g := colMax(y,yy,k)")
		assert.Nil(t, e)
		exp := []int{1, 2, 16, 4, 15, 14}
		assert.ElementsMatch(t, exp, dfx.Column("g").Data().AsAny())

		e = d.Parse(dfx, "l := colMin(y,yy,k)")
		assert.Nil(t, e)
		exp = []int{1, -15, 3, 1, 4, 5}
		assert.ElementsMatch(t, exp, dfx.Column("l").Data().AsAny())

		e = d.Parse(dfx, "g := colMax(s,x)")
		assert.Nil(t, e)
		expF := []float64{2, -2, 6, 0, 4, 7}
		assert.ElementsMatch(t, expF, dfx.Column("g").Data().AsAny())

		e = d.Parse(dfx, "l := colMin(s, x)")
		assert.Nil(t, e)
		expF = []float64{1, -4, 3, 0, 2, 3.5}
		assert.ElementsMatch(t, expF, dfx.Column("l").Data().AsAny())

		e = d.Parse(dfx, "g := colMax(z, v)")
		assert.Nil(t, e)
		expS := []string{"20221231", "20040101", "20060102", "20060102", "20230915", "20060310"}
		assert.ElementsMatch(t, expS, dfx.Column("g").Data().AsAny())

		e = d.Parse(dfx, "g := colMax(d, dt)")
		assert.Nil(t, e)
		var expD1 []time.Time
		for ind := range len(expS) {
			t, _ := time.Parse("20060102", expS[ind])
			expD1 = append(expD1, t)
		}

		assert.ElementsMatch(t, expD1, dfx.Column("g").Data().AsAny())

		e = d.Parse(dfx, "l := colMin(z, v)")
		assert.Nil(t, e)
		expS = []string{"20040101", "20000101", "20040101", "20040101", "20040101", "20040101"}
		assert.ElementsMatch(t, expS, dfx.Column("l").Data().AsAny())

		e = d.Parse(dfx, "l := colMin(d, dt)")
		assert.Nil(t, e)
		var expD []time.Time
		for ind := range len(expS) {
			t, _ := time.Parse("20060102", expS[ind])
			expD = append(expD, t)
		}

		assert.ElementsMatch(t, expD, dfx.Column("l").Data().AsAny())
	}
}

func TestX(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		e := d.Parse(dfx, "test:='hello'' world'")
		assert.Nil(t, e)
		fmt.Println(dfx.Column("test").Data().AsAny(), which)
		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestCast(t *testing.T) {
	for _, which := range pkgs("d1") {
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestTemp(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(pg + ",d1")
		fmt.Println("WHICH ", which)
		for row, r := range dfx.AllRows() {
			fmt.Println("row: ", row)
			fmt.Println(r)
		}

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}

	/*
	   	for col := range dfx.Core().AllColumns() {
	   		fmt.Println(col.Name())
	   	}

	   outDF, e0 := dfx.By("y", "sx:=sum(x)", "count:=count(y)", "mx:=mean(global(x))", "mz:=mean(x)")
	   assert.Nil(t, e0)
	   d := outDF.Column("mx").Data().AsAny()
	   fmt.Println(d)
	*/
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
	colx, ex := m.NewCol(vx, d.ColName("x"))
	assert.Nil(t, ex)
	coly, ey := m.NewCol(vy, d.ColName("y"))
	assert.Nil(t, ey)
	colz, ez := m.NewCol(vz, d.ColName("z"))
	assert.Nil(t, ez)
	cols, es := m.NewCol(vs, d.ColName("s"))
	assert.Nil(t, es)
	//	coldt, edt := m.NewCol(vdt, d.ColName("dt"))
	//	assert.Nil(t, edt)
	df, ed := m.NewDFcol([]*m.Col{colx, coly, colz, cols})
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
	for _, which := range pkgs("d1") {
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestRowNumber(t *testing.T) {
	for _, which := range pkgs("d1") {
		fmt.Println(which)
		dfx := loadData(which)
		e := d.Parse(dfx, "rn:=rowNumber()")
		assert.Nil(t, e)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, dfx.Column("rn").Data().AsAny())

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestIf(t *testing.T) {
	for _, which := range pkgs("d1") {
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestParser(t *testing.T) {
	for _, which := range pkgs("d1") {
		dfx := loadData(which)
		if !strings.Contains(which, "click") {
			continue
		}
		tests := strings.Split(parserTests, "\n")
		for _, test := range tests {
			//			fmt.Println(test, which)
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
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

// TODO: check for referencing elements directly not through method
// TODO: revisit sourceDF relative to mem/By

// TODO: optional name to DFSeq for the column

// TODO: are there more func parameters to get rid of?
// TODO: convert %s to #0, #1,... in functions.txt

// TODO: hats & one-hot

// TODO: can I combine a file reader and an sql save to read from csv & write directly to a table
/*

date functions
  ageDays(begDate, endDate)
XX  ageMonths(begDate, endDate)
XX  ageYears(begDate, endDate)
  addDays(begDate, days)
XX  addMonths(begDate, months)
X  addYears(begDate, years)
XX  toEndOfMonth()
  today()
XX  month()
XX  day()
XX  year()
XX  dayOfWeek()
XX  makeDate()

string functions:
XX  position
XX  replace
XX  substring


math:
  isNan
  isInf
  isNull

statistical
  pdf,cdf
  empirical quantile, cdf?
  random numbers
     uniform
	 N(0,1)
	 Exp(1)
	 Binomial(n,p)

vector
  diff
  lag
  lead

OneHot(col,baseName) - add to df
Hat(col,knots,baseName) - add to df


*/

func TestToCat(t *testing.T) {
	for _, which := range pkgs("d1") {
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

func TestApplyCat(t *testing.T) {
	for _, which := range pkgs("d1") {
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

		if dlct := dfx.Dialect(); dlct != nil {
			_ = dlct.Close()
		}
	}
}

// TODO: think about float -> string
// TODO: think about date formats
