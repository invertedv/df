package df

import (
	"fmt"
	"math"
)

func MemRun(fn *Func, params []any, inputs []Column) (outCol Column, err error) {
	if len(inputs)+len(params) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		xOut    any
		outType DataTypes
	)
	for ind := 0; ind < inputs[0].Len(); ind++ {
		var xs []any

		for j := 0; j < len(params); j++ {
			xadd, e := toDataType(params[j], fn.inputs[j], true)
			if e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		for j := 0; j < len(inputs); j++ {
			xadd, e := toDataType(inputs[j].(*MemCol).Element(ind), fn.inputs[j+len(params)], false)
			if e != nil {
				return nil, e
			}

			xs = append(xs, xadd)
		}

		x, _, e := fn.function(xs...)
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

func memCast(inputs ...any) (any, DataTypes, error) {
	dt := DTFromString(inputs[0].(string))

	x, e := toDataType(inputs[1], dt, true)
	return x, dt, e
}

func memAdd(inputs ...any) (any, DataTypes, error) {
	dt0 := whatAmI(inputs[0])
	dt1 := whatAmI(inputs[1])

	switch {
	case dt0 == DTfloat && dt1 == DTfloat:
		return inputs[0].(float64) + inputs[1].(float64), DTfloat, nil
	case dt0 == DTfloat && dt1 == DTint:
		return inputs[0].(float64) + float64(inputs[1].(int)), DTfloat, nil
	case dt0 == DTint && dt1 == DTfloat:
		return float64(inputs[0].(int)) + inputs[1].(float64), DTfloat, nil
	case dt0 == DTint && dt1 == DTint:
		return inputs[0].(int) + inputs[1].(int), DTint, nil
	case dt0 == DTstring:
		if s, e := toString(inputs[1], true); e == nil {
			return inputs[0].(string) + s.(string), DTstring, nil
		}
	case dt1 == DTstring:
		if s, e := toString(inputs[0], true); e == nil {
			return s.(string) + inputs[1].(string), DTstring, nil
		}
	}

	return nil, DTunknown, fmt.Errorf("cannot add %s and %s", dt0, dt1)
}

func memExp(xs ...any) (any, DataTypes, error) {
	return math.Exp(xs[0].(float64)), DTfloat, nil
}

func memAbs(inputs ...any) (any, DataTypes, error) {
	switch x := inputs[0].(type) {
	case float64:
		return math.Abs(x), DTfloat, nil
	case int:
		if x < 0 {
			return -x, DTint, nil
		}
		return x, DTint, nil
	default:
		return nil, DTunknown, fmt.Errorf("abs requires float or int")
	}
}
