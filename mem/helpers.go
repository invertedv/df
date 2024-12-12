package df

import (
	"fmt"

	d "github.com/invertedv/df"
)

// mean

// buildTests builds suite of comparison function (>, <, >=, <=, ==, !=) for the four
// core data types (dtFloat, dtInt, dtString,dtDate).
func buildTests() [][]func(x ...*d.Atomic) int {
	// build "greater than" functions for each type
	var fnGTtype []func(x ...*d.Atomic) bool
	for ind := 0; ind < 4; ind++ {
		fn := func(x ...*d.Atomic) bool {
			switch ind {
			case 0:
				return *x[0].AsFloat() > *x[1].AsFloat()
			case 1:
				return *x[0].AsInt() > *x[1].AsInt()
			case 2:
				return *x[0].AsString() > *x[1].AsString()
			default:
				return x[0].AsDate().Sub(*x[1].AsDate()).Minutes() > 0
			}
		}

		fnGTtype = append(fnGTtype, fn)
	}

	// build all comparison functions for each type leveraging fnGTtype slice
	var fns [][]func(x ...*d.Atomic) int
	for comp := 0; comp < 6; comp++ {
		var fnDt []func(x ...*d.Atomic) int
		for dt := 0; dt < len(fnGTtype); dt++ {
			fn := func(x ...*d.Atomic) int {
				a, b := x[0], x[1]
				switch comp {
				case 0:
					return bint(fnGTtype[dt](a, b)) // a > b
				case 1:
					return bint(fnGTtype[dt](b, a)) // a < b
				case 2:
					return bint(!fnGTtype[dt](b, a)) // a >= b
				case 3:
					return bint(!fnGTtype[dt](a, b)) // a <= b
				case 4:
					return bint(!fnGTtype[dt](a, b) && !fnGTtype[dt](b, a)) // a == b
				default:
					return bint(fnGTtype[dt](a, b) || fnGTtype[dt](b, a)) // a!=b
				}
			}

			fnDt = append(fnDt, fn)
		}

		fns = append(fns, fnDt)
	}

	return fns
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
		if c, e = NewCol(s.Data(), d.ColName(s.Name())); e != nil {
			panic(e)
		}

		return c
	}

	panic("can't make column")
}

// TODO: here
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

	if outCol, e = NewCol(data); e != nil {
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

func bint(x bool) int {
	if x {
		return 1
	}
	return 0
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
