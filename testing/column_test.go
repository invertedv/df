package testing

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	m "github.com/invertedv/df/mem"

	d "github.com/invertedv/df"
	"github.com/stretchr/testify/assert"
)

type vector[T Supported] struct {
	dt   d.DataTypes
	data []T
}

func (v *vector[T]) VectorType() d.DataTypes {
	return v.dt
}

func (v *vector[T]) AsFloat() []float64 {
	if v.dt == d.DTfloat {
		var x any
		x = v.data
		return x.([]float64)
	}

	return nil
}

type Supported interface {
	float64 | int | string | time.Time
	//	Less(i, j int) bool
}

type slc[S Supported] []S

func (v *vector[S]) Less(i, j int) bool {
	switch r := any(v.data).(type) {
	case []float64:
		return r[i] < r[j]
	case slc[int]:
		return r[i] < r[j]
	case slc[string]:
		return r[i] < r[j]
	case slc[time.Time]:
		return r[i].Sub(r[j]).Seconds() < 0
	}

	return false
}

func sum[S ~float64 | ~int](x []S) S {
	var sumx S = 0.0
	for _, xx := range x {
		sumx += xx
	}

	return sumx
}

func newVector[T Supported](data []T, n int) *vector[T] {
	v := &vector[T]{data: data}
	v.dt = d.WhatAmI(v.data[0])

	if n > 0 {
		for ind := 1; ind < n; ind++ {
			v.data = append(v.data, v.data[0])
		}
	}

	return v
}

func makeVector[T Supported](n int) *vector[T] {
	datx := make([]T, n)
	v := newVector[T](datx, 0)

	return v
}

func xyz[T Supported](v *vector[T]) {
	fmt.Println(v.data[0])
}

type vecs interface {
	*vector[float64] | *vector[int] | *vector[string] | *vector[time.Time]
}

func ttt(x ...any) bool {
	for ind := 0; ind < len(x); ind++ {
		switch x[ind].(type) {
		case *vector[float64]:
			fmt.Println("float")
		case *vector[int]:
			fmt.Println("int")
		case *vector[string]:
			fmt.Println("string")
		case *vector[time.Time]:
			fmt.Println("date")
		}
	}
	z := x[0]
	_ = z
	return false
}

func ok1(x any) (int, bool) {
	fn := func(x reflect.Value) (int, bool) {
		if x.CanInt() {
			return int(x.Int()), true
		}

		fmt.Println("NO")
		return -1, false
	}

	return fn(reflect.ValueOf(x))
}

// TODO: replace Any2* with these
// TODO: add checks for slices in NewVector using reflect

func toInt(x any) (any, bool) {
	if i, ok := x.(int); ok {
		return i, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return int(xv.Int()), true
	}

	// TODO: check can this ever be true?
	if xv.CanFloat() {
		return int(xv.Float()), true
	}

	if s, ok := x.(string); ok {
		if i, e := strconv.ParseInt(s, 10, 64); e == nil {
			return int(i), true
		}
	}

	return nil, false
}

func toFloat(x any) (any, bool) {
	if f, ok := x.(float64); ok {
		return f, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanFloat() {
		return xv.Float(), true
	}

	// TODO: check can this ever be true?
	if xv.CanInt() {
		return float64(xv.Int()), true
	}

	if s, ok := x.(string); ok {
		if f, e := strconv.ParseFloat(s, 64); e == nil {
			return f, true
		}
	}

	return nil, false
}

func toString(x any) (any, bool) {
	if s, ok := x.(string); ok {
		return s, true
	}

	if f, ok := x.(float64); ok {
		return fmt.Sprintf("%0.3f", f), true
	}

	if i, ok := x.(int); ok {
		return fmt.Sprintf("%d", i), true
	}

	if s, ok := x.(time.Time); ok {
		return s.Format("2006-01-02"), true
	}

	return nil, false
}

func toDate(x any) (any, bool) {
	if d, ok := x.(time.Time); ok {
		return d, true
	}

	xv := reflect.ValueOf(x)
	if xv.CanInt() {
		return toDate(fmt.Sprintf("%d", xv.Int()))
	}

	if d, ok := x.(string); ok {
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			if dt, e := time.Parse(fmtx, strings.ReplaceAll(d, "'", "")); e == nil {
				return dt, true
			}
		}
	}

	return nil, false
}

func toSlc(xIn any, target d.DataTypes) (any, bool) {
	typSlc := []reflect.Type{reflect.TypeOf([]float64{}), reflect.TypeOf([]int{}), reflect.TypeOf([]string{""}), reflect.TypeOf([]time.Time{})}
	toFns := []func(a any) (any, bool){toFloat, toInt, toString, toDate}

	x := reflect.ValueOf(xIn)

	var indx int
	switch target {
	case d.DTfloat:
		indx = 0
	case d.DTint:
		indx = 1
	case d.DTstring:
		indx = 2
	case d.DTdate:
		indx = 3
	default:
		return nil, false
	}

	outType := typSlc[indx]

	// nothing to do
	if x.Type() == outType {
		return xIn, true
	}

	toFn := toFns[indx]
	var xOut reflect.Value
	if x.Kind() == reflect.Slice {
		for ind := 0; ind < x.Len(); ind++ {
			r := x.Index(ind).Interface()
			if ind == 0 {
				xOut = reflect.MakeSlice(outType, x.Len(), x.Len())
			}
			var (
				val any
				ok  bool
			)

			if val, ok = toFn(r); !ok {
				return nil, false
			}

			xOut.Index(ind).Set(reflect.ValueOf(val))

		}

		return xOut.Interface(), true
	}

	// input is not a slice:
	if val, ok := toFn(xIn); ok {
		xOut = reflect.MakeSlice(outType, 1, 1)
		xOut.Index(0).Set(reflect.ValueOf(val))
		return xOut.Interface(), true
	}

	return nil, false
}

func TestT(t *testing.T) {
	d1 := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	if d1 == d2 {
		fmt.Println("Dates the same")
	}
	fmt.Println(toSlc([]string{"20060102", "20201231"}, d.DTfloat))
	fmt.Println(toSlc(1.23331, d.DTstring))
	n := 1000 //00000
	z := make([]float32, n)
	for ind := int(0); ind < int(n); ind++ {
		//zx, _ := toInt(ind)
		z[ind] = float32(ind) + 0.1234 //zx.(int)
	}
	tm := time.Now()
	zout, _ := toSlc(z, d.DTfloat)
	fmt.Println(time.Since(tm).Seconds(), "Seconds")
	_ = zout
	ok1(1)
	ok1(int32(1))
	ok1("hello")
	x := make([]float64, 10)
	for ind := 0; ind < 10; ind++ {
		x[ind] = 4.0 - float64(ind)
	}
	y := newVector[float64](x, 0)
	fmt.Println(y)
	xyz(y)
	fmt.Println(1, 2, y.Less(1, 2))
	fmt.Println(2, 1, y.Less(2, 1))
	//	fmt.Println(sum(y.data))
	r := reflect.TypeOf(y.data[0])
	fmt.Println(r.Kind())

}

// TODO: test min/max for string & date <---------------
func TestRandom(t *testing.T) {
	n := 100000000
	x := make([]float64, n)
	y := make([]float64, n)
	z := make([]float64, n)
	s := make([]string, n)
	//	dt := make([]time.Time, n)
	for ind := 0; ind < n; ind++ {
		x[ind] = float64(-ind + 1)
		y[ind] = float64(-ind + 1)
		z[ind] = float64(ind)
		s[ind] = fmt.Sprintf("%d", ind)
		//		s[ind] = fmt.Sprintf("%d", ind)
		//		l := ind % 40
		//		dt[ind] = time.Date(1980+l, 6, 1, 0, 0, 0, 0, time.UTC)

	}

	vx := d.NewVector(x, d.DTfloat)
	vy := d.NewVector(y, d.DTfloat)
	vz := d.NewVector(z, d.DTfloat)
	vs := d.NewVector(s, d.DTstring)
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
	outCol, ep := d.Parse(df, "x")
	//	outCol, ep := d.Parse(df, "dot(y,y)")
	assert.Nil(t, ep)

	fmt.Println("value ", outCol.AsColumn().(*m.Col).Element(0))
	fmt.Println(time.Since(tx).Seconds(), " seconds")
	tx = time.Now()
	//	m := x[0]
	//	for ind := 0; ind < n; ind++ {
	//		//z[ind] = x[ind] - y[ind] // x[ind] + y[ind]
	//		if x[ind] > m {
	//			m = x[ind]
	//		}
	//	}
	for ind := 0; ind < n; ind++ {
		//z[ind] = x[ind] - y[ind] // x[ind] + y[ind]
		z[ind] = x[ind]
	}

	fmt.Println(time.Since(tx).Seconds(), " seconds")
	_ = outCol
}

func TestRename(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		x1 := dfx.Column("x").Data()
		d.ColName("xa")(dfx.Column("x"))
		d.ColName("xb")(dfx.Column("xa"))
		assert.Equal(t, x1, dfx.Column("xb").Data())

	}
}

func TestRowNumber(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		out, e := d.Parse(dfx, "rowNumber()")
		assert.Nil(t, e)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, out.AsColumn().Data().AsInt())
	}
}

func TestReplace(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)
		indCol, e0 := d.Parse(dfx, "y==-5")
		assert.Nil(t, e0)
		d.ColName("ind")(indCol.AsColumn())
		e := dfx.Core().AppendColumn(indCol.AsColumn(), false)
		assert.Nil(t, e)
		coly := dfx.Column("y")
		assert.NotNil(t, coly)
		colyy := dfx.Column("yy")
		assert.NotNil(t, colyy)
		//		colR, e3 := coly.Replace(indCol.AsColumn(), colyy)
		//		assert.Nil(t, e3)
		//		assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, colR.Data())

		// via Parse
		out, e4 := d.Parse(dfx, "if(y==-5,yy,y)")
		assert.Nil(t, e4)
		assert.Equal(t, []int{1, -15, 6, 1, 4, 5}, out.AsColumn().Data().AsInt())
	}
}

func TestParser(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		x := [][]any{
			{"sum(y)", 0, 12},
			{"(x/0.1)", 0, 10.0},
			{"y+100", 0, 101.0},
			{"(x/0.1)*float(y+100)", 0, 1010.0},
			{"z!='20060102'", 0, 1},
			{"dt != date(20221231)", 0, 0},
			{"y+y", 0, 2},
			{"date('20221231')", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
			{"quantile(1.0,y)", 0, 6},
			{"quantile(0.0,y)", 0, -5},
			{"max(dt)", 0, time.Date(2023, 9, 15, 0, 0, 0, 0, time.UTC)},
			{"min(dt)", 0, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
			{"min(z)", 0, "20000101"},
			{"max(z)", 0, "20230915"},
			{"min(y)", 0, -5},
			{"max(y)", 0, 6},
			{"min(x)", 0, -2.0},
			{"max(x)", 0, 3.5},
			{"quantile(0.5,y)", 0, 1},
			{"quantile(0.5,x)", 0, 1.0},
			{"median(y)", 0, 1},
			{"median(x)", 0, 1.0},
			{"sdev(y)", 0, 4.0},
			{"sdev(x)", 0, 2.043},
			{"var(y)", 0, 16.0},
			{"var(x)", 0, 4.175},
			{"y > 2", 5, 1},
			{"y > 2", 0, 0},
			{"y+y", 1, -10},
			{"rowNumber()", 1, 1},
			{"abs(yy)", 1, 15},
			{"sqrt(x)", 4, 1.414},
			{"dot(x,x)", 0, 30.25},
			{"mean(x)", 0, 1.25},
			{"x--3.0", 0, 4.0},
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
			{"date(20221231)", 0, time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
			{"dt != date(20221231)", 1, 1},
			{"dt == date(20221231)", 0, 1},
			{"dt == date(20221231)", 1, 0},
			//			{"string(float(1)+.234)", 0, "1.234"},
			{"float('1.1')", 0, 1.1},
			{"int(2.9)", 0, 2},
			{"float(1)", 0, 1.0},
			{"string(dt)", 0, "2022-12-31"},
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
			cnt++
			eqn := x[ind][0].(string)
			fmt.Println(eqn)
			xOut, e := d.Parse(dfx, eqn)
			assert.Nil(t, e)
			result := xOut.AsColumn().Data()
			switch d.WhatAmI(result) {
			case d.DTfloat:
				assert.InEpsilon(t, x[ind][2].(float64), result.AsFloat()[x[ind][1].(int)], .001)
			case d.DTint:
				assert.Equal(t, x[ind][2], result.AsInt()[x[ind][1].(int)])
			case d.DTstring:
				assert.Equal(t, x[ind][2], result.AsString()[x[ind][1].(int)])
			case d.DTdate:
				val := result.AsDate()[x[ind][1].(int)]
				assert.Equal(t, val.Year(), x[ind][2].(time.Time).Year())
				assert.Equal(t, val.Month(), x[ind][2].(time.Time).Month())
				assert.Equal(t, val.Day(), x[ind][2].(time.Time).Day())
			}
		}
	}
}

// TODO: consider dropping cat counts
func TestToCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		colx, e := d.Parse(dfx, "date(z)")
		assert.Nil(t, e)
		col := colx.AsColumn()
		d.ColName("dt1")(col)
		e = dfx.Core().AppendColumn(col, false)
		assert.Nil(t, e)

		//		colx, e = dfx.Parse("1")
		//		col = colx.AsColumn()
		//		e = col.Rename("howdy")
		//		e = dfx.AppendColumn(col, false)
		// try with DTint
		colx, e = d.Parse(dfx, "cat(y)")
		assert.Nil(t, e)
		d.ColName("test")(colx.AsColumn())
		result := colx.AsColumn().Data()
		expected := []int{1, 0, 4, 1, 2, 3}
		assert.Equal(t, expected, result.AsInt())

		// try with DTstring
		colx, e = d.Parse(dfx, "cat(z)")
		assert.Nil(t, e)
		result = colx.AsColumn().Data()
		expected = []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expected, result.AsInt())

		// try with DTdate
		colx, e = d.Parse(dfx, "cat(dt1)")
		assert.Nil(t, e)
		result = colx.AsColumn().Data()
		expected = []int{3, 0, 1, 1, 4, 2}
		assert.Equal(t, expected, result.AsInt())

		// try with fuzz > 1
		colx, e = d.Parse(dfx, "cat(y, 2)")
		assert.Nil(t, e)
		result = colx.AsColumn().Data()
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, result.AsInt())

		// try with DTfloat
		_, e = d.Parse(dfx, "cat(x)")
		assert.NotNil(t, e)
	}
}

func TestApplyCat(t *testing.T) {
	for _, which := range pkgs() {
		dfx := loadData(which)

		r, e := d.Parse(dfx, "cat(y)")
		assert.Nil(t, e)
		sx := r.AsColumn()
		d.ColName("caty")(sx)
		e1 := dfx.Core().AppendColumn(sx, false)
		assert.Nil(t, e1)

		r2, e2 := d.Parse(dfx, "applyCat(yy, caty, -5)")
		assert.Nil(t, e2)

		// -5 maps to 0 so all new values map to 0
		expected := []int{1, 0, 0, 1, 0, 0}
		assert.Equal(t, expected, r2.AsColumn().Data().AsInt())

		// try with fuzz > 1
		r3, e3 := d.Parse(dfx, "cat(y,2)")
		assert.Nil(t, e3)
		d.ColName("caty2")(r3.AsColumn())
		e4 := dfx.Core().AppendColumn(r3.AsColumn(), false)
		assert.Nil(t, e4)

		r5, e5 := d.Parse(dfx, "applyCat(yy,caty2,-5)")
		assert.Nil(t, e5)
		expected = []int{0, -1, -1, 0, -1, -1}
		assert.Equal(t, expected, r5.AsColumn().Data().AsInt())
	}
}
