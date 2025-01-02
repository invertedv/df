package df

import (
	"fmt"
	"time"

	d "github.com/invertedv/df"
)

// TODO: move to vector_functions
func compare[T float64 | int | string | time.Time](n int, x, y []T, comp func(a, b T) bool) (*d.Vector, error) {
	z := make([]int, n)
	inc1, inc2 := 1, 1
	if len(x) == 1 {
		inc1 = 0
	}

	if len(y) == 1 {
		inc2 = 0
	}

	ind1, ind2 := 0, 0
	for ind := 0; ind < n; ind++ {
		if comp(x[ind1], y[ind2]) {
			z[ind] = 1
		}
		ind1 += inc1
		ind2 += inc2
	}

	// should not fail
	v, _ := d.NewVector(z, d.WhatAmI(z[0]))
	return v, nil
}

// buildTests builds suite of comparison function (>, <, >=, <=, ==, !=) for the four
// core data types (dtFloat, dtInt, dtString,dtDate).
func buildTests() [][]func(x ...any) (*d.Vector, error) {
	// build "greater than" functions for each type
	fltCmp := []func(a, b float64) bool{
		func(a, b float64) bool { return a > b },
		func(a, b float64) bool { return a < b },
		func(a, b float64) bool { return a >= b },
		func(a, b float64) bool { return a <= b },
		func(a, b float64) bool { return a == b },
		func(a, b float64) bool { return a != b },
	}
	intCmp := []func(a, b int) bool{
		func(a, b int) bool { return a > b },
		func(a, b int) bool { return a < b },
		func(a, b int) bool { return a >= b },
		func(a, b int) bool { return a <= b },
		func(a, b int) bool { return a == b },
		func(a, b int) bool { return a != b },
	}
	stringCmp := []func(a, b string) bool{
		func(a, b string) bool { return a > b },
		func(a, b string) bool { return a < b },
		func(a, b string) bool { return a >= b },
		func(a, b string) bool { return a <= b },
		func(a, b string) bool { return a == b },
		func(a, b string) bool { return a != b },
	}
	dateCmp := []func(a, b time.Time) bool{
		func(a, b time.Time) bool { return a.After(b) },
		func(a, b time.Time) bool { return a.Before(b) },
		func(a, b time.Time) bool { return a.After(b) || a.Equal(b) },
		func(a, b time.Time) bool { return a.Before(b) || a.Equal(b) },
		func(a, b time.Time) bool { return a.Equal(b) },
		func(a, b time.Time) bool { return !a.Equal(b) },
	}

	var flts, ints, strs, dts []func(x ...any) (*d.Vector, error)

	for ind := 0; ind < len(fltCmp); ind++ {
		flts = append(flts, func(x ...any) (*d.Vector, error) {
			n, x1, x2 := x[0].(int), x[1].([]float64), x[2].([]float64)
			return compare(n, x1, x2, fltCmp[ind])
		})
		ints = append(ints, func(x ...any) (*d.Vector, error) {
			n, x1, x2 := x[0].(int), x[1].([]int), x[2].([]int)
			return compare(n, x1, x2, intCmp[ind])
		})
		strs = append(strs, func(x ...any) (*d.Vector, error) {
			n, x1, x2 := x[0].(int), x[1].([]string), x[2].([]string)
			return compare(n, x1, x2, stringCmp[ind])
		})
		dts = append(dts, func(x ...any) (*d.Vector, error) {
			n, x1, x2 := x[0].(int), x[1].([]time.Time), x[2].([]time.Time)
			return compare(n, x1, x2, dateCmp[ind])
		})
	}

	return [][]func(x ...any) (*d.Vector, error){flts, ints, strs, dts}
}

func toCol(x any) *Col {
	if c, ok := x.(*Col); ok {
		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var (
			c *Col
			e error
		)
		if c, e = NewCol(s.Data(), s.DataType(), d.ColName(s.Name())); e != nil {
			panic(e)
		}

		return c
	}

	panic("can't make column")
}

func parameters(inputs ...d.Column) (cols []*Col, n int) {
	n = 1
	for j := 0; j < len(inputs); j++ {
		cx := toCol(inputs[j])
		cols = append(cols, cx)

		if nn := cx.Len(); nn > n {
			n = nn
		}
	}

	return cols, n
}

func returnCol(data any) *d.FnReturn {
	var (
		outCol *Col
		e      error
	)

	// TODO: this may not work
	if outCol, e = NewCol(data, d.WhatAmI(data)); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

// getNames returns the names of the input Columns starting with startInd element
func getNames(startInd int, cols ...d.Column) ([]string, error) {
	var colNames []string
	for ind := startInd; ind < len(cols); ind++ {
		var cn string
		if cn = cols[ind].(*Col).Name(); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

func signature(target [][]d.DataTypes, cols ...*Col) int {
	for j := 0; j < len(target); j++ {
		ind := j
		for k := 0; k < len(target[j]); k++ {
			if target[j][k] != cols[k].DataType() {
				ind = -1
				break
			}
		}

		if ind >= 0 {
			return ind
		}
	}

	return -1
}

func has[C comparable](needle C, haystack []C) bool {
	for _, straw := range haystack {
		if needle == straw {
			return true
		}
	}

	return false
}
