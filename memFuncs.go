package df

import (
	"fmt"
	"math"
)

type MemFunc struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function AnyFunction
}

type MemFuncMap map[string]*MemFunc

var Functions = memLoadFunctions()

func (fn *MemFunc) Run(inputs ...any) (outCol Column, err error) {
	if len(inputs) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		vals   []*MemCol
		params []any
	)

	for ind := 0; ind < len(inputs); ind++ {
		var (
			col *MemCol
			ok  bool
		)

		if col, ok = inputs[ind].(*MemCol); ok {
			vals = append(vals, col)
		} else {
			params = append(params, inputs[ind])
		}
	}

	var (
		xOut    any
		outType DataTypes
	)
	for ind := 0; ind < vals[0].Len(); ind++ {
		var xs []any

		for j := 0; j < len(params); j++ {
			xadd, e := toDataType(params[j], fn.inputs[j], true)
			if e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		for j := 0; j < len(vals); j++ {
			xadd, e := toDataType(vals[j].Element(ind), fn.inputs[j+len(params)], false)
			if e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		x, e := fn.function(xs...)
		if e != nil {
			return nil, e
		}

		if ind == 0 {
			outType = whatAmI(x)
			if fn.output != DTany && fn.output != outType {
				panic("function return not required type")
			}
		}

		if whatAmI(x) != outType {
			panic("inconsistent function return types")
		}

		// or have Run return a type?
		if ind == 0 {
			xOut = makeSlice(outType)
		}

		xOut = appendSlice(xOut, x, outType)
	}

	outCol = &MemCol{
		name:   "",
		dType:  outType,
		data:   xOut,
		catMap: nil,
	}

	return outCol, nil
}

func memLoadFunctions() MemFuncMap {
	fns := make(MemFuncMap)
	names, inputs, outputs, fnsx := funcDetails(true)
	for ind := 0; ind < len(names); ind++ {
		fns[names[ind]] = &MemFunc{
			name:     names[ind],
			inputs:   inputs[ind],
			output:   outputs[ind],
			function: fnsx[ind],
		}
	}

	return fns
}

func memCast(inputs ...any) (any, error) {
	dt := DTFromString(inputs[0].(string))

	return toDataType(inputs[1], dt, true)
}

func memAdd(inputs ...any) (any, error) {
	dt0 := whatAmI(inputs[0])
	dt1 := whatAmI(inputs[1])

	switch {
	case dt0 == DTfloat && dt1 == DTfloat:
		return inputs[0].(float64) + inputs[1].(float64), nil
	case dt0 == DTfloat && dt1 == DTint:
		return inputs[0].(float64) + float64(inputs[1].(int)), nil
	case dt0 == DTint && dt1 == DTfloat:
		return float64(inputs[0].(int)) + inputs[1].(float64), nil
	case dt0 == DTint && dt1 == DTint:
		return inputs[0].(int) + inputs[1].(int), nil
	case dt0 == DTstring:
		if s, e := toString(inputs[1], true); e == nil {
			return inputs[0].(string) + s.(string), nil
		}
	case dt1 == DTstring:
		if s, e := toString(inputs[0], true); e == nil {
			return s.(string) + inputs[1].(string), nil
		}
	}

	return nil, fmt.Errorf("cannot add %s and %s", dt0, dt1)
}

func memExp(xs ...any) (any, error) {
	return math.Exp(xs[0].(float64)), nil
}

func memAbs(xs ...any) (any, error) { return math.Abs(xs[0].(float64)), nil }
