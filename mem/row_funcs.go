package df

import (
	"fmt"
	"math"
	"time"

	d "github.com/invertedv/df"
)

func RunDFfn(fn d.Fn, context *d.Context, inputs []any) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	for j := 0; j < len(inputs); j++ {
		var (
			ok bool
		)

		// fix this up...don't need mod. Use something other than c
		_, ok = inputs[j].(*MemCol)
		if !ok {
			return nil, fmt.Errorf("input to function %s is not a Column", info.Name)
		}
	}

	var fnR *d.FnReturn
	if fnR = fn(false, context, inputs...); fnR.Err != nil {
		return nil, fnR.Err
	}

	return fnR.Value.(d.Column), nil
}

func RunRowFn(fn d.Fn, context *d.Context, inputs []any) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	var (
		xOut    any
		outType d.DataTypes
	)

	n := *context.Len()
	for ind := 0; ind < n; ind++ {
		var xs []any

		for j := 0; j < len(inputs); j++ {
			var (
				xadd any
				e    error
			)

			// fix this up...don't need mod. Use something other than c
			if cx, ok := inputs[j].(*MemCol); ok {
				if xadd, e = d.ToDataType(cx.Element(ind), info.Inputs[j], false); e != nil {
					return nil, e
				}

				xs = append(xs, xadd)
				continue
			}

			if xadd, e = d.ToDataType(inputs[j], info.Inputs[j], true); e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		var fnr *d.FnReturn
		if fnr = fn(false, nil, xs...); fnr.Err != nil {
			return nil, fnr.Err
		}

		if ind == 0 {
			outType = fnr.Output
			if info.Output != d.DTany && info.Output != outType {
				return nil, fmt.Errorf("inconsistent function return types: got %s need %s", info.Output, outType)
			}
		}

		var dt d.DataTypes
		if dt = d.WhatAmI(fnr.Value); dt != outType {
			return nil, fmt.Errorf("inconsistent function return types: got %s need %s", dt, outType)
		}

		if dt == d.DTnone {
			continue
		}

		if ind == 0 {
			xOut = d.MakeSlice(outType, 0, nil)
		}

		xOut = d.AppendSlice(xOut, fnr.Value, outType)
	}

	outCol = &MemCol{
		name:   "",
		dType:  outType,
		data:   xOut,
		catMap: nil,
	}

	return outCol, nil
}

func StandardFunctions() d.Fns {
	return d.Fns{
		abs, add, and, cast, divide,
		eq, exp, ge, gt, ifs, le, log, lt,
		multiply, ne, not, or, subtract, sum,
		toDate, toFloat, toInt, toString}
}

// /////// Standard Fns

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	switch x := inputs[0].(type) {
	case float64:
		return &d.FnReturn{Value: math.Abs(x), Output: d.DTfloat, Err: nil}
	case int:
		if x < 0 {
			return &d.FnReturn{Value: -x, Output: d.DTint, Err: nil}
		}
		return &d.FnReturn{Value: x, Output: d.DTint, Err: nil}
	default:
		return &d.FnReturn{Value: nil, Output: d.DTunknown, Err: fmt.Errorf("abs requires float or int")}
	}
}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "add", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) + float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) + inputs[1].(int), Output: d.DTint, Err: nil}
	case dt0 == d.DTstring:
		if s, e := d.ToString(inputs[1], true); e == nil {
			return &d.FnReturn{Value: inputs[0].(string) + s.(string), Output: d.DTstring, Err: nil}
		}
	case dt1 == d.DTstring:
		if s, e := d.ToString(inputs[0], true); e == nil {
			return &d.FnReturn{Value: s.(string) + inputs[1].(string), Output: d.DTstring, Err: nil}
		}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot add %s and %s", dt0, dt1)}
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "and", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	val := 0
	if inputs[0].(int) > 0 && inputs[1].(int) > 0 {
		val = 1
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "or", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	val := 0
	if inputs[0].(int) > 0 || inputs[1].(int) > 0 {
		val = 1
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func cast(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var dt d.DataTypes
	if dt = d.DTFromString(inputs[0].(string)); dt == d.DTunknown {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	x, e := d.ToDataType(inputs[1], dt, true)
	return &d.FnReturn{Value: x, Output: dt, Err: e}
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "divide", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) / float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) / inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot divide %s and %s", dt0, dt1)}
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "eq", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("==", inputs[0], inputs[1])
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ge", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare(">=", inputs[0], inputs[1])
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "gt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare(">", inputs[0], inputs[1])
}

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FnReturn{Value: math.Exp(inputs[0].(float64)), Output: d.DTfloat, Err: nil}
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "if", Inputs: []d.DataTypes{d.DTint, d.DTany, d.DTany}, Output: d.DTany}
	}

	if inputs[0].(int) > 0 {
		return &d.FnReturn{Value: inputs[1], Output: d.WhatAmI(inputs[1])}
	}

	return &d.FnReturn{Value: inputs[2], Output: d.WhatAmI(inputs[2])}
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "le", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("<=", inputs[0], inputs[1])
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	x := inputs[0].(float64)
	if x <= 0 {
		return &d.FnReturn{Err: fmt.Errorf("log of non-positive number")}
	}

	return &d.FnReturn{Value: math.Log(x), Output: d.DTfloat, Err: nil}
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "lt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("<", inputs[0], inputs[1])
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) * float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) * inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot multiply %s and %s", dt0, dt1)}
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ne", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}

	return compare("!=", inputs[0], inputs[1])
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "not", Inputs: []d.DataTypes{d.DTany, d.DTint}, Output: d.DTint}
	}

	val := 1
	if inputs[1].(int) > 0 {
		val = 0
	}

	return &d.FnReturn{Value: val, Output: d.DTint}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "subtract", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FnReturn{Value: inputs[0].(float64) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(float64) - float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FnReturn{Value: float64(inputs[0].(int)) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FnReturn{Value: inputs[0].(int) - inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FnReturn{Value: nil, Err: fmt.Errorf("cannot subtract %s and %s", dt0, dt1)}
}

func makeMCvalue(val any, dt d.DataTypes) *MemCol {
	data := d.MakeSlice(dt, 1, nil)
	switch dt {
	case d.DTfloat:
		data.([]float64)[0] = val.(float64)
	case d.DTint:
		data.([]int)[0] = val.(int)
	case d.DTstring:
		data.([]string)[0] = val.(string)
	case d.DTdate:
		data.([]time.Time)[0] = val.(time.Time)
	}

	var (
		outCol *MemCol
		e      error
	)

	if outCol, e = NewMemCol("", data); e != nil {
		panic("probelm in makeMCvalue")
	}

	return outCol
}

func sum(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sum", Inputs: []d.DataTypes{d.DTany}, Output: d.DTany, Scalar: true}
	}

	col := inputs[0].(*MemCol)
	dt := col.DataType()
	data := col.Data()
	if !dt.IsNumeric() {
		return &d.FnReturn{Err: fmt.Errorf("input to sum must be numeric, got %v", dt)}
	}

	sf := d.InitAny(dt)

	for ind := 0; ind < col.Len(); ind++ {
		switch col.DataType() {
		case d.DTfloat:
			x := sf.(float64)
			x += data.([]float64)[ind]
			sf = x
		case d.DTint:
			x := sf.(int)
			x += data.([]int)[ind]
			sf = x
		default:
			return &d.FnReturn{Err: fmt.Errorf("invalid type in sum")}
		}
	}

	outCol := makeMCvalue(sf, dt)

	return &d.FnReturn{Value: outCol, Output: dt, Err: nil}
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "date", Output: d.DTdate, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTdate", inputs[0])
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "float", Output: d.DTfloat, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTfloat", inputs[0])
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "int", Output: d.DTint, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTint", inputs[0])
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "string", Output: d.DTstring, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTstring", inputs[0])
}

///////// helpers

func compare(condition string, left, right any) *d.FnReturn {
	var truth bool
	ret := &d.FnReturn{Value: int(0), Output: d.DTint}
	if truth, ret.Err = d.Comparator(left, right, condition); truth {
		ret.Value = int(1)
	}

	return ret
}
