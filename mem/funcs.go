package df

import (
	"fmt"
	l "log"
	"math"

	d "github.com/invertedv/df"
)

func Run(fn d.AnyFunction, inputs []any) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), info.Name, len(info.Inputs))
	}

	var (
		xOut    any
		outType d.DataTypes
	)

	n := 1
	for _, inp := range inputs {
		if c, ok := inp.(*MemCol); ok {
			if nc := c.Len(); nc > n {
				n = nc
			}
		}
	}

	for ind := 0; ind < n; ind++ {
		var xs []any

		for j := 0; j < len(inputs); j++ {
			var (
				xadd any
				e    error
			)

			if c, ok := inputs[j].(*MemCol); ok {
				indx := ind % c.Len()
				if ind >= c.Len() && c.Len() > 1 {
					l.Println("warning unequal lengths in MemDF")
				}

				if xadd, e = d.ToDataType(c.Element(indx), info.Inputs[j], false); e != nil {
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
		if fnr = fn(false, xs...); fnr.Err != nil {
			return nil, fnr.Err
		}

		if ind == 0 {
			outType = d.WhatAmI(fnr.Value)
			if info.Output != d.DTany && info.Output != outType {
				panic("function return not required type")
			}
		}

		if d.WhatAmI(fnr.Value) != outType {
			panic("inconsistent function return types")
		}

		// or have Run return a type?
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
	return d.Functions{exp, abs, cast, add, log}
}

///////// Standard Functions

func cast(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	dt := d.DTFromString(inputs[0].(string))

	x, e := d.ToDataType(inputs[1], dt, true)
	return &d.FuncReturn{Value: x, Output: dt, Err: e}
}

func add(info bool, inputs ...any) *d.FuncReturn {
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

func exp(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FuncReturn{Value: math.Exp(inputs[0].(float64)), Output: d.DTfloat, Err: nil}
}

func abs(info bool, inputs ...any) *d.FuncReturn {
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

func log(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	x := inputs[0].(float64)
	if x <= 0 {
		return &d.FuncReturn{Err: fmt.Errorf("log of non-positive number")}
	}

	return &d.FuncReturn{Value: math.Log(x), Output: d.DTfloat, Err: nil}
}
