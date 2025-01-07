package df

import (
	"fmt"
	"math"
	"time"

	d "github.com/invertedv/df"
)

func vector(name string, inp [][]d.DataTypes, outp []d.DataTypes, fnx ...any) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
		}

		fnUse := fnx[0]
		n := df.RowCount()
		var col []*Col
		if inp != nil {
			col, n = parameters(inputs...)
			ind := signature(inp, col...)
			if ind < 0 {
				panic("no signature")
			}

			fnUse = fnx[ind]
		}

		inData := getVecs(n, col...)
		var (
			data *d.Vector
			err  error
		)
		if data, err = fnUse.(func(x ...any) (*d.Vector, error))(inData...); err != nil {
			return &d.FnReturn{Err: err}
		}

		return returnCol(data)
	}

	return fn
}

func getVecs(n int, cols ...*Col) []any {
	v := []any{n}
	for ind := 0; ind < len(cols); ind++ {
		v = append(v, cols[ind].Data().AsAny())
	}

	return v
}

func castOps() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTcategorical}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTstring}, {d.DTdate}}
	inType3 := [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}}

	out := d.Fns{
		vector("float", inType1, []d.DataTypes{d.DTfloat, d.DTfloat, d.DTfloat, d.DTfloat},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]float64), d.DTfloat), nil
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]int), d.DTint).Coerce(d.DTfloat)
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]string), d.DTstring).Coerce(d.DTfloat)
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]int), d.DTint).Coerce(d.DTfloat)
			},
		),
		vector("int", inType1, []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]float64), d.DTfloat).Coerce(d.DTint)
			},
			func(x ...any) (*d.Vector, error) { return newVector(x[1].([]int), d.DTint), nil },
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]string), d.DTstring).Coerce(d.DTint)
			},
			func(x ...any) (*d.Vector, error) { return newVector(x[1].([]int), d.DTint), nil },
		),
		vector("string", inType2, []d.DataTypes{d.DTstring, d.DTstring, d.DTstring, d.DTstring},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]float64), d.DTfloat).Coerce(d.DTstring)
			},
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]int), d.DTint).Coerce(d.DTstring)
			},
			func(x ...any) (*d.Vector, error) { return newVector(x[1].([]string), d.DTstring), nil },
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]time.Time), d.DTdate).Coerce(d.DTstring)
			},
		),
		vector("date", inType3, []d.DataTypes{d.DTdate, d.DTdate, d.DTdate},
			func(x ...any) (*d.Vector, error) { return newVector(x[1].([]int), d.DTint).Coerce(d.DTdate) },
			func(x ...any) (*d.Vector, error) {
				return newVector(x[1].([]string), d.DTstring).Coerce(d.DTdate)
			},
			func(x ...any) (*d.Vector, error) { return newVector(x[1].([]time.Time), d.DTdate), nil },
		)}

	return out
}

func coreMathFn[T float64 | int](op func(a T) T, x ...any) (*d.Vector, error) {
	n, x1 := x[0].(int), x[1].([]T)
	xOut := make([]T, n)
	for ind, xv := range x1 {
		xOut[ind] = op(xv)
	}

	return newVector(xOut, d.WhatAmI(xOut[0])), nil
}

func abs[T float64 | int](x T) T {
	if x >= 0 {
		return x
	}

	return -x
}

func mathFuncs() d.Fns {
	inType1 := [][]d.DataTypes{{d.DTfloat}}
	inType2 := [][]d.DataTypes{{d.DTfloat}, {d.DTint}}
	outType1 := []d.DataTypes{d.DTfloat}
	outType2 := []d.DataTypes{d.DTfloat, d.DTint}

	out := d.Fns{
		vector("exp", inType1, outType1, func(x ...any) (*d.Vector, error) { return coreMathFn[float64](math.Exp, x...) }),
		vector("log", inType1, outType1, func(x ...any) (*d.Vector, error) { return coreMathFn[float64](math.Log, x...) }),
		vector("sqrt", inType1, outType1, func(x ...any) (*d.Vector, error) { return coreMathFn[float64](math.Sqrt, x...) }),
		vector("abs", inType2, outType2, func(x ...any) (*d.Vector, error) { return coreMathFn[float64](abs, x...) },
			func(x ...any) (*d.Vector, error) { return coreMathFn[int](abs, x...) }),
		vector("neg", inType2, outType2, func(x ...any) (*d.Vector, error) {
			return coreMathFn[float64](func(a float64) float64 { return -a }, x...)
		},
			func(x ...any) (*d.Vector, error) { return coreMathFn[int](func(a int) int { return -a }, x...) }),
	}

	return out
}

func logic(n int, x, y []int, test func(a, b *int) bool) (*d.Vector, error) {
	z := make([]int, n)
	inc1, inc2 := 1, 1
	if len(x) == 1 {
		inc1 = 0
	}

	if len(y) == 1 {
		inc2 = 0
	}

	ind1, ind2 := 0, 0
	var yp *int
	yp = nil
	for ind := 0; ind < n; ind++ {
		// handles "not" which has only 1 arg
		if y != nil {
			yp = &y[ind2]
		}
		if test(&x[ind1], yp) {
			z[ind] = 1
		}
		ind1 += inc1
		ind2 += inc2
	}

	return newVector(z, d.DTint), nil
}

func logicalOps() d.Fns {
	inType2 := [][]d.DataTypes{{d.DTint, d.DTint}}
	inType1 := [][]d.DataTypes{{d.DTint}}
	outType := []d.DataTypes{d.DTint}

	and := func(x ...any) (*d.Vector, error) {
		n, x1, x2 := x[0].(int), x[1].([]int), x[2].([]int)
		return logic(n, x1, x2, func(a, b *int) bool { return *a > 0 && *b > 0 })
	}
	or := func(x ...any) (*d.Vector, error) {
		n, x1, x2 := x[0].(int), x[1].([]int), x[2].([]int)
		return logic(n, x1, x2, func(a, b *int) bool { return *a > 0 || *b > 0 })
	}
	not := func(x ...any) (*d.Vector, error) {
		n, x1 := x[0].(int), x[1].([]int)
		return logic(n, x1, nil, func(a, b *int) bool { return *a <= 0 })
	}
	out := d.Fns{
		vector("and", inType2, outType, and),
		vector("or", inType2, outType, or),
		vector("not", inType1, outType, not)}

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
			vector(op, inType, outType, fns[0][ind], fns[1][ind], fns[2][ind], fns[3][ind]))
	}

	return out
}

func mathFn[T float64 | int](op func(a, b T) (T, error), xIn ...any) (*d.Vector, error) {
	n, x, y := xIn[0].(int), xIn[1].([]T), xIn[2].([]T)
	inc1, inc2 := 1, 1
	if len(x) == 1 {
		inc1 = 0
	}

	if len(y) == 1 {
		inc2 = 0
	}

	z := make([]T, n)
	ind1, ind2 := 0, 0
	var e error
	for ind := 0; ind < n; ind++ {
		if z[ind], e = op(x[ind1], y[ind2]); e != nil {
			return nil, e
		}

		ind1 += inc1
		ind2 += inc2
	}

	return newVector(z, d.WhatAmI(z[0])), nil
}

func add[T float64 | int](a, b T) (T, error)  { return a + b, nil }
func sub[T float64 | int](a, b T) (T, error)  { return a - b, nil }
func mult[T float64 | int](a, b T) (T, error) { return a * b, nil }
func div[T float64 | int](a, b T) (T, error) {
	if b != 0 {
		return a / b, nil
	}
	return 0, fmt.Errorf("divide by 0")
}

func mathOps() d.Fns {
	inType := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}}
	outType := []d.DataTypes{d.DTfloat, d.DTint}

	out := d.Fns{
		vector("add", inType, outType, func(x ...any) (*d.Vector, error) { return mathFn[float64](add, x...) },
			func(x ...any) (*d.Vector, error) { return mathFn[int](add, x...) }),
		vector("subtract", inType, outType, func(x ...any) (*d.Vector, error) { return mathFn[float64](sub, x...) },
			func(x ...any) (*d.Vector, error) { return mathFn[int](sub, x...) }),
		vector("multiply", inType, outType, func(x ...any) (*d.Vector, error) { return mathFn[float64](mult, x...) },
			func(x ...any) (*d.Vector, error) { return mathFn[int](mult, x...) }),
		vector("divide", inType, outType, func(x ...any) (*d.Vector, error) { return mathFn[float64](div, x...) },
			func(x ...any) (*d.Vector, error) { return mathFn[int](div, x...) }),
	}

	return out
}

func ifx[T float64 | int | string | time.Time](xIn ...any) (*d.Vector, error) {
	n, cond, x, y := xIn[0].(int), xIn[1].([]int), xIn[2].([]T), xIn[3].([]T)
	z := make([]T, n)
	inc0, inc1, inc2 := 1, 1, 1
	if len(cond) == 1 {
		inc0 = 0
	}
	if len(x) == 1 {
		inc1 = 0
	}
	if len(y) == 1 {
		inc2 = 0
	}

	ind0, ind1, ind2 := 0, 0, 0
	for ind := 0; ind < n; ind++ {
		if cond[ind0] > 0 {
			z[ind] = x[ind1]
		} else {
			z[ind] = y[ind2]
		}
		ind0 += inc0
		ind1 += inc1
		ind2 += inc2
	}

	return newVector(z, d.WhatAmI(z[0])), nil
}

// ifOp implements the if statement
func ifOp() d.Fn {
	inType := [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat}, {d.DTint, d.DTint, d.DTint},
		{d.DTint, d.DTstring, d.DTstring}, {d.DTint, d.DTdate, d.DTdate},
	}
	outType := []d.DataTypes{d.DTfloat, d.DTint, d.DTstring, d.DTdate}

	return vector("if", inType, outType,
		func(x ...any) (*d.Vector, error) { return ifx[float64](x...) },
		func(x ...any) (*d.Vector, error) { return ifx[int](x...) },
		func(x ...any) (*d.Vector, error) { return ifx[string](x...) },
		func(x ...any) (*d.Vector, error) { return ifx[time.Time](x...) })
}

func otherVectors() d.Fns {
	outType1 := []d.DataTypes{d.DTint}
	outType2 := []d.DataTypes{d.DTfloat}

	rn := func(x ...any) (*d.Vector, error) {
		n := x[0].(int)
		outX := make([]int, n)
		for ind := 0; ind < n; ind++ {
			outX[ind] = ind
		}

		return newVector(outX, d.WhatAmI(outX[0])), nil
	}

	ev := func(x ...any) (*d.Vector, error) {
		return newVector(math.E, d.DTfloat), nil
	}

	pInf := func(x ...any) (*d.Vector, error) {
		return newVector(math.Inf(1), d.DTfloat), nil
	}

	mInf := func(x ...any) (*d.Vector, error) {
		return newVector(math.Inf(-1), d.DTfloat), nil
	}

	out := d.Fns{
		vector("rowNumber", nil, outType1, rn),
		vector("e", nil, outType2, ev),
		vector("pInf", nil, outType2, pInf),
		vector("mInf", nil, outType2, mInf),
		ifOp()}

	return out
}

// ***************** Categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
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
		f, _ := c.ElementInt(0)
		fuzz = *f
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = df.Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(dt)(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// applyCat
// - vector to apply cats to
// - vector with cats
// - default if new category
// TODO: should the default be an existing category?
func applyCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
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
	if outCol, e = df.Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

func newVector(data any, dt d.DataTypes) *d.Vector {
	var (
		x *d.Vector
		e error
	)
	if x, e = d.NewVector(data, dt); e != nil {
		panic(e)
	}

	return x
}
