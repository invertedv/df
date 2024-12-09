package df

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	d "github.com/invertedv/df"
)

// TODO: what happens if try to compare to dtCategorical??

func vector(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, context *d.Context, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		fnUse := fnx[0]
		n := context.Self().RowCount()
		dtOut := outp[0]
		var col []*Col
		if inp != nil {
			col, n = parameters(inputs...)
			ind := signature(inp, col...)
			if ind < 0 {
				panic("no signature")
			}

			fnUse = fnx[ind]
			dtOut = outp[ind]
		}

		data := d.MakeVector(dtOut, n)
		switch {
		case dtOut == d.DTfloat:
			fny := fnUse.(func(...any) float64)
			for ind := 0; ind < n; ind++ {
				data.SetFloat(fny(row(ind, col...)...), ind)
			}
		case dtOut == d.DTint:
			fny := fnUse.(func(...any) int)
			for ind := 0; ind < n; ind++ {
				data.SetInt(fny(row(ind, col...)...), ind)
			}
		case dtOut == d.DTstring:
			fny := fnUse.(func(...any) string)
			for ind := 0; ind < n; ind++ {
				data.SetString(fny(row(ind, col...)...), ind)
			}
		case dtOut == d.DTdate:
			fny := fnUse.(func(...any) time.Time)
			for ind := 0; ind < n; ind++ {
				data.SetDate(fny(row(ind, col...)...), ind)
			}
		}

		return returnCol(data)
	}

	return fn
}

// row returns the ind row from cols
func row(ind int, cols ...*Col) []any {
	if cols == nil {
		return []any{ind}
	}

	outRow := make([]any, len(cols))
	for j := 0; j < len(cols); j++ {
		outRow[j] = cols[j].Element(ind)
	}

	return outRow
}

func castOps() d.Fns {
	const (
		bitSizeF = 64
		bitSizeI = 64
		base     = 10
		dtFmt    = "2006-01-02"
	)

	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTcategorical}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}}
	inType3 := [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}}

	asDate := func(x string) time.Time {
		formats := []string{"20060102", "1/2/2006", "01/02/2006", "Jan 2, 2006", "January 2, 2006", "Jan 2 2006", "January 2 2006", "2006-01-02"}
		for _, fmtx := range formats {
			dt, e := time.Parse(fmtx, strings.ReplaceAll(x, "'", ""))
			if e == nil {
				return dt
			}
		}

		panic("cannot convert to date")
	}

	out := d.Fns{
		vector("float", inType1, []d.DataTypes{d.DTfloat, d.DTfloat, d.DTfloat, d.DTfloat},
			func(x ...any) float64 { return x[0].(float64) },
			func(x ...any) float64 { return float64(x[0].(int)) },
			func(x ...any) float64 {
				if xf, e := strconv.ParseFloat(x[0].(string), bitSizeF); e == nil {
					return xf
				}
				panic("cannot convert string to float")
			},
			func(x ...any) float64 { return float64(x[0].(int)) },
		),
		vector("int", inType1, []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint},
			func(x ...any) int { return int(x[0].(float64)) },
			func(x ...any) int { return x[0].(int) },
			func(x ...any) int {
				if xi, e := strconv.ParseInt(x[0].(string), base, bitSizeI); e == nil {
					return int(xi)
				}
				panic("cannot convert string to int")
			},
			func(x ...any) int { return x[0].(int) },
		),
		vector("string", inType2, []d.DataTypes{d.DTstring, d.DTstring, d.DTstring, d.DTstring},
			// TODO: build smarter choice for # decimals
			func(x ...any) string { v := x[0].(float64); return fmt.Sprintf("%0.3f", v) },
			func(x ...any) string { return fmt.Sprintf("%d", x[0].(int)) },
			func(x ...any) string { return x[0].(string) },
			func(x ...any) string { return x[0].(time.Time).Format(dtFmt) }),
		vector("date", inType3, []d.DataTypes{d.DTdate, d.DTdate, d.DTdate},
			func(x ...any) time.Time { return asDate(fmt.Sprintf("%d", x[0].(int))) },
			func(x ...any) time.Time { return asDate(x[0].(string)) },
			func(x ...any) time.Time { return x[0].(time.Time) })}

	return out
}

func mathFuncs() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outType1 := []d.DataTypes{d.DTfloat}
	outType2 := []d.DataTypes{d.DTfloat, d.DTint}

	absInt := func(x ...any) int {
		xx := x[0].(int)
		if xx >= 0 {
			return xx
		}

		return -xx
	}

	out := d.Fns{
		vector("exp", inType1, outType1, func(x ...any) float64 { return math.Exp(x[0].(float64)) }),
		vector("log", inType1, outType1, func(x ...any) float64 { return math.Log(x[0].(float64)) }),
		vector("sqrt", inType1, outType1, func(x ...any) float64 { return math.Sqrt(x[0].(float64)) }),
		vector("abs", inType2, outType2, func(x ...any) float64 { return math.Abs(x[0].(float64)) }, absInt),
	}

	return out
}

func logicalOps() d.Fns {
	inType2 := [][]d.DataTypes{{d.DTint, d.DTint}}
	inType1 := [][]d.DataTypes{{d.DTint}}
	outType := []d.DataTypes{d.DTint}
	out := d.Fns{
		vector("and", inType2, outType, func(x ...any) int { return bint(x[0].(int) > 0 && x[1].(int) > 0) }),
		vector("or", inType2, outType, func(x ...any) int { return bint(x[0].(int) > 0 || x[1].(int) > 0) }),
		vector("not", inType1, outType, func(x ...any) int { return 1 - bint(x[0].(int) > 0) })}

	return out
}

func comparisons() d.Fns {
	inType := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint},
		{d.DTstring, d.DTstring}, {d.DTdate, d.DTdate},
	}

	fns := buildTests()

	outType := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}

	var out d.Fns
	for ind, op := range []string{"gt", "lt", "ge", "le", "eq", "ne"} {
		out = append(out,
			vector(op, inType, outType, fns[ind][0], fns[ind][1], fns[ind][2], fns[ind][3]))
	}

	return out
}

func mathOps() d.Fns {
	inType2 := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}}
	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outType := []d.DataTypes{d.DTfloat, d.DTint}

	addFloat := func(x ...any) float64 {
		return x[0].(float64) + x[1].(float64)
	}
	addInt := func(x ...any) int {
		return x[0].(int) + x[1].(int)
	}
	subFloat := func(x ...any) float64 { return x[0].(float64) - x[1].(float64) }
	subInt := func(x ...any) int { return x[0].(int) - x[1].(int) }
	multFloat := func(x ...any) float64 { return x[0].(float64) * x[1].(float64) }
	multInt := func(x ...any) int { return x[0].(int) * x[1].(int) }
	divFloat := func(x ...any) float64 { return x[0].(float64) / x[1].(float64) }
	divInt := func(x ...any) int { return x[0].(int) / x[1].(int) }
	negFloat := func(x ...any) float64 { return -x[0].(float64) }
	negInt := func(x ...any) int { return -x[0].(int) }

	out := d.Fns{
		vector("add", inType2, outType, addFloat, addInt),
		vector("divide", inType2, outType, divFloat, divInt),
		vector("multiply", inType2, outType, multFloat, multInt),
		vector("subtract", inType2, outType, subFloat, subInt),
		vector("neg", inType1, outType, negFloat, negInt)}

	return out
}

func otherVectors() d.Fns {
	outType := []d.DataTypes{d.DTint}

	out := d.Fns{
		vector("rowNumber", nil, outType, func(x ...any) int { return x[0].(int) }),
		ifOp()}

	return out
}

// ifOp implements the if statement
func ifOp() d.Fn {
	inType := [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat}, {d.DTint, d.DTint, d.DTint},
		{d.DTint, d.DTstring, d.DTstring}, {d.DTint, d.DTdate, d.DTdate},
	}
	outType := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate}

	iFloat64 := func(x ...any) float64 {
		if x[0].(int) > 0 {
			return x[1].(float64)
		}
		return x[2].(float64)
	}
	iInt := func(x ...any) int {
		if x[0].(int) > 0 {
			return x[1].(int)
		}
		return x[2].(int)
	}
	iString := func(x ...any) string {
		if x[0].(int) > 0 {
			return x[1].(string)
		}
		return x[2].(string)
	}
	iDate := func(x ...any) time.Time {
		if x[0].(int) > 0 {
			return x[1].(time.Time)
		}
		return x[2].(time.Time)
	}

	return vector("if", inType, outType, iFloat64, iInt, iString, iDate)
}

// ***************** Functions that take a single column and return a scalar *****************

// ***************** Categorical Operations *****************

func toCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*Col)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	fuzz := 1
	if len(inputs) > 1 {
		c := toCol(inputs[1])
		if c.DataType() != d.DTint {
			return &d.FnReturn{Err: fmt.Errorf("fuzz parameter to Cat must be type int")}
		}
		fuzz = c.Element(0).(int)
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = context.Self().(*DF).Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	//	outCol.(*Col).rawType = dt
	d.ColRawType(dt)(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
// TODO: should the default be an existing category?
func applyCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := toCol(inputs[0])
	oldData := toCol(inputs[1])
	newVal := toCol(inputs[2])

	if newData.DataType() != oldData.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	var (
		defaultValue any
		e            error
	)

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	defaultValue = newVal.Element(0)

	var levels []any
	for k := range oldData.CategoryMap() {
		levels = append(levels, k)
	}

	var outCol d.Column
	if outCol, e = context.Self().(*DF).Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	//	outCol.(*Col).RawType() = newData.DataType()
	d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}
