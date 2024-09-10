package sql

import (
	"fmt"
	d "github.com/invertedv/df"
)

func Run(fn d.RowFn, context *d.Context, inputs []any) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	var xs []any
	for ind := 0; ind < len(inputs); ind++ {
		if cx, ok := inputs[ind].(*SQLcol); ok {
			xs = append(xs, cx.Name(""))
			continue
		}

		// check if value can be cast to correct type
		if _, e := d.ToDataType(inputs[ind], info.Inputs[ind], true); e != nil {
			return nil, e
		}

		xs = append(xs, inputs[ind].(string))
	}

	r := fn(false, context, xs...)

	if r.Err != nil {
		return nil, r.Err
	}

	outCol = &SQLcol{
		name:   "",
		dType:  r.Output,
		sql:    r.Value.(string),
		catMap: nil,
	}

	return outCol, nil
}

func StandardFunctions() d.RowFns {
	return d.RowFns{abs, add, c, cast, divide, exp, log, ifs, multiply, subtract, toFloat, toInt}
}

// ////////  Standard RowFns

func abs(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", inputs[0].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func add(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s + %s", inputs[0].(string), inputs[1].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func multiply(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s * %s", inputs[0].(string), inputs[1].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func divide(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "divide", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s / %s", inputs[0].(string), inputs[1].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s - %s", inputs[0].(string), inputs[1].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func c(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "c", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var dt d.DataTypes
	if dt = d.DTFromString(inputs[0].(string)); dt == d.DTunknown {
		return &d.RowFnReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	var (
		val string
		ex  error
	)
	if val, ex = context.Dialect().CastConstant(inputs[1].(string), dt); ex != nil {
		return &d.RowFnReturn{Err: ex}
	}

	return &d.RowFnReturn{Value: val, Output: dt}
}

func cast(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	var toDT d.DataTypes
	if toDT = d.DTFromString(inputs[0].(string)); toDT == d.DTunknown {
		return &d.RowFnReturn{Err: fmt.Errorf("unknown data type %s", inputs[0].(string))}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(inputs[1].(string), toDT); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: toDT}
}

func toInt(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "int", Inputs: []d.DataTypes{d.DTany}, Output: d.DTint}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(inputs[1].(string), d.DTint); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "float", Inputs: []d.DataTypes{d.DTany}, Output: d.DTfloat}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(inputs[1].(string), d.DTfloat); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTfloat}
}

func toString(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "string", Inputs: []d.DataTypes{d.DTany}, Output: d.DTstring}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(inputs[1].(string), d.DTstring); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTstring}
}

func exp(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("exp(%s)", inputs[0].(string))

	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func ifs(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "if", Inputs: []d.DataTypes{d.DTstring, d.DTany, d.DTany}, Output: d.DTint}
	}

	ret := &d.RowFnReturn{Output: d.DTint}
	ret.Value, ret.Err = context.Dialect().Ifs(inputs[1].(string), inputs[2].(string), inputs[0].(string))

	return ret
}

func log(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("log(%s)", inputs[0].(string))
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}
