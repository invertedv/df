package testing

import (
	"fmt"
	"math"
	"testing"
	"time"

	d "github.com/invertedv/df"
	"github.com/stretchr/testify/assert"
)

type intp int

func (i intp) Less(x intp) bool {
	return i < x
}

type Orderable[T intp | float64] interface {
	Less(a T) bool
}

func TestRandom(t *testing.T) {
	a := float64(1.0)
	b := intp(2)
	_, _ = a, b
	var i interface{} = b
	if _, ok := i.(Orderable[intp]); ok {
		fmt.Println("YES")
	}

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
