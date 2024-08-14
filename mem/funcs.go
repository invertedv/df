package df

import (
	"fmt"
	"math"

	d "github.com/invertedv/df"
)

var Functions = d.FunctionList{exp, abs, cast, add}

func Run(fn *d.Func, params []any, inputs []d.Column) (outCol d.Column, err error) {
	if len(inputs)+len(params) != len(fn.Inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.Name, len(fn.Inputs))
	}

	var (
		xOut    any
		outType d.DataTypes
	)

	for ind := 0; ind < inputs[0].Len(); ind++ {
		var xs []any

		for j := 0; j < len(params); j++ {
			var (
				xadd any
				e    error
			)

			if xadd, e = d.ToDataType(params[j], fn.Inputs[j], true); e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		for j := 0; j < len(inputs); j++ {
			var (
				xadd any
				e    error
			)

			if xadd, e = d.ToDataType(inputs[j].(*MemCol).Element(ind), fn.Inputs[j+len(params)], false); e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		var fnr *d.FuncReturn
		if fnr = fn.Function(xs...); fnr.Err != nil {
			return nil, fnr.Err
		}

		if ind == 0 {
			outType = d.WhatAmI(fnr.Value)
			if fn.Output != d.DTany && fn.Output != outType {
				panic("function return not required type")
			}
		}

		if d.WhatAmI(fnr.Value) != outType {
			panic("inconsistent function return types")
		}

		// or have Run return a type?
		if ind == 0 {
			xOut = d.MakeSlice(outType)
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

func cast(inputs ...any) *d.FuncReturn {
	myName := "cast"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	dt := d.DTFromString(inputs[0].(string))

	x, e := d.ToDataType(inputs[1], dt, true)
	return &d.FuncReturn{Value: x, DT: dt, Name: myName, Err: e}
}

func add(inputs ...any) *d.FuncReturn {
	myName := "add"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	dt0 := d.WhatAmI(inputs[0])
	dt1 := d.WhatAmI(inputs[1])

	switch {
	case dt0 == d.DTfloat && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: inputs[0].(float64) + inputs[1].(float64), DT: d.DTfloat, Name: myName, Err: nil}
	case dt0 == d.DTfloat && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(float64) + float64(inputs[1].(int)), DT: d.DTfloat, Name: myName, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTfloat:
		return &d.FuncReturn{Value: float64(inputs[0].(int)) + inputs[1].(float64), DT: d.DTfloat, Name: myName, Err: nil}
	case dt0 == d.DTint && dt1 == d.DTint:
		return &d.FuncReturn{Value: inputs[0].(int) + inputs[1].(int), DT: d.DTint, Name: myName, Err: nil}
	case dt0 == d.DTstring:
		if s, e := d.ToString(inputs[1], true); e == nil {
			return &d.FuncReturn{Value: inputs[0].(string) + s.(string), DT: d.DTstring, Name: myName, Err: nil}
		}
	case dt1 == d.DTstring:
		if s, e := d.ToString(inputs[0], true); e == nil {
			return &d.FuncReturn{Value: s.(string) + inputs[1].(string), DT: d.DTstring, Name: myName, Err: nil}
		}
	}

	return &d.FuncReturn{Value: nil, DT: d.DTunknown, Name: myName, Err: fmt.Errorf("cannot add %s and %s", dt0, dt1)}
}

func exp(xs ...any) *d.FuncReturn {
	myName := "exp"
	if xs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}
	return &d.FuncReturn{Value: math.Exp(xs[0].(float64)), DT: d.DTfloat, Name: myName, Err: nil}
}

func abs(inputs ...any) *d.FuncReturn {
	myName := "abs"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	switch x := inputs[0].(type) {
	case float64:
		return &d.FuncReturn{Value: math.Abs(x), DT: d.DTfloat, Name: myName, Err: nil}
	case int:
		if x < 0 {
			return &d.FuncReturn{Value: -x, DT: d.DTint, Name: myName, Err: nil}
		}
		return &d.FuncReturn{Value: x, DT: d.DTint, Name: myName, Err: nil}
	default:
		return &d.FuncReturn{Value: nil, DT: d.DTunknown, Name: myName, Err: fmt.Errorf("abs requires float or int")}
	}
}
