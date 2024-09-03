package df

import (
	"fmt"
	"math"

	d "github.com/invertedv/df"
)

func Run(fn d.AnyFunction, context *d.Context, inputs []any) (outCol d.Column, err error) {
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

		var fnr *d.FuncReturn
		if fnr = fn(false, nil, xs...); fnr.Err != nil {
			return nil, fnr.Err
		}

		if ind == 0 {
			outType = fnr.Output
			if info.Output != d.DTany && info.Output != outType {
				return nil, fmt.Errorf("inconsistent function return types: got %s need %s", info.Output, outType)
			}
		}

		if dt := d.WhatAmI(fnr.Value); dt != outType {
			return nil, fmt.Errorf("inconsistent function return types: got %s need %s", dt, outType)
		}

		if ind == 0 {
			xOut = d.MakeSlice(outType, 0)
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

func StandardFunctions() d.Functions {
	return d.Functions{abs, add, c, cast, divide, exp, log, ifs, multiply, subtract, toFloat, toInt}
}

// /////// Standard Functions

func where(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "if", Inputs: []d.DataTypes{d.DTstring, d.DTany, d.DTany}, Output: d.DTint}
	}

	return &d.FuncReturn{}
}

func abs(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	switch x := inputs[0].(type) {
	case float64:
		return &d.FuncReturn{Value: math.Abs(x), Output: d.DTfloat, Err: nil}
	case int:
		if x < 0 {
			return &d.FuncReturn{Value: -x, Output: d.DTint, Err: nil}
		}
		return &d.FuncReturn{Value: x, Output: d.DTint, Err: nil}
	default:
		return &d.FuncReturn{Value: nil, Output: d.DTunknown, Err: fmt.Errorf("abs requires float or int")}
	}
}

func add(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "add", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: inputs[0].(float64) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(float64) + float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: float64(inputs[0].(int)) + inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(int) + inputs[1].(int), Output: d.DTint, Err: nil}
	case dt0 == d.DTstring:
		if s, e := d.ToString(inputs[1], true); e == nil {
			return &d.FuncReturn{Value: inputs[0].(string) + s.(string), Output: d.DTstring, Err: nil}
		}
	case dt1 == d.DTstring:
		if s, e := d.ToString(inputs[0], true); e == nil {
			return &d.FuncReturn{Value: s.(string) + inputs[1].(string), Output: d.DTstring, Err: nil}
		}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot add %s and %s", dt0, dt1)}
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: inputs[0].(float64) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(float64) * float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: float64(inputs[0].(int)) * inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(int) * inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot multiply %s and %s", dt0, dt1)}
}

func divide(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "divide", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: inputs[0].(float64) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(float64) / float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: float64(inputs[0].(int)) / inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(int) / inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot divide %s and %s", dt0, dt1)}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "subtract", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTany}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: inputs[0].(float64) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(float64) - float64(inputs[1].(int)), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: float64(inputs[0].(int)) - inputs[1].(float64), Output: d.DTfloat, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(int) - inputs[1].(int), Output: d.DTint, Err: nil}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot subtract %s and %s", dt0, dt1)}
}

func c(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "c", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var (
		x  any
		dt d.DataTypes
		e  error
	)

	if dt = d.DTFromString(inputs[0].(string)); dt == d.DTunknown {
		return &d.FuncReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	if x, e = d.ToDataType(inputs[1], dt, true); e != nil {
		return &d.FuncReturn{Err: e}
	}

	return &d.FuncReturn{Value: x, Output: dt}
}

func cast(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var dt d.DataTypes
	if dt = d.DTFromString(inputs[0].(string)); dt == d.DTunknown {
		return &d.FuncReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	x, e := d.ToDataType(inputs[1], dt, true)
	return &d.FuncReturn{Value: x, Output: dt, Err: e}
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "int", Output: d.DTint, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTint", inputs[0])
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "float", Output: d.DTfloat, Inputs: []d.DataTypes{d.DTany}}
	}

	return cast(false, context, "DTfloat", inputs[0])
}

func exp(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FuncReturn{Value: math.Exp(inputs[0].(float64)), Output: d.DTfloat, Err: nil}
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "if", Inputs: []d.DataTypes{d.DTstring, d.DTany, d.DTany}, Output: d.DTint}
	}

	var truth bool
	ret := &d.FuncReturn{Value: int(0), Output: d.DTint}
	if truth, ret.Err = d.Comparator(inputs[1], inputs[2], inputs[0].(string)); truth {
		ret.Value = int(1)
	}

	return ret
}

func log(info bool, context *d.Context, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	x := inputs[0].(float64)
	if x <= 0 {
		return &d.FuncReturn{Err: fmt.Errorf("log of non-positive number")}
	}

	return &d.FuncReturn{Value: math.Log(x), Output: d.DTfloat, Err: nil}
}
