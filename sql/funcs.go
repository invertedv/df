package sql

import (
	"fmt"
	d "github.com/invertedv/df"
)

func Run(fn d.AnyFunction, inputs []any) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs) != len(info.Inputs) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	var xs []any
	for ind := 0; ind < len(inputs); ind++ {
		var (
			xadd any
			e    error
		)

		if c, ok := inputs[ind].(*SQLcol); ok {
			xs = append(xs, c)
			continue
		}

		if xadd, e = d.ToDataType(inputs[ind], info.Inputs[ind], true); e != nil {
			return nil, e
		}

		xs = append(xs, xadd)
	}

	r := fn(false, xs...)

	if r.Err != nil {
		return nil, err
	}

	outCol = &SQLcol{
		name:   "",
		dType:  r.Output,
		sql:    r.Value.(string),
		catMap: nil,
	}

	return outCol, nil
}

func StandardFunctions() d.Functions {
	return d.Functions{exp, abs, cast, add, log, c}
}

// ////////  Standard Functions
func abs(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "abs", Inputs: []d.DataTypes{d.DTany, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", inputs[1].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func add(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "add", Inputs: []d.DataTypes{d.DTany, d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s + %s", inputs[1].(d.Column).Name(""), inputs[2].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func c(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "c", Inputs: []d.DataTypes{d.DTany, d.DTstring, d.DTany}, Output: d.DTany}
	}

	var dt d.DataTypes
	if dt = d.DTFromString(inputs[1].(string)); dt == d.DTunknown {
		return &d.FuncReturn{Err: fmt.Errorf("cannot convert to %s", inputs[0].(string))}
	}

	dialect := inputs[0].(*d.Dialect)
	var (
		sql string
		e   error
	)

	if sql, e = dialect.CastConstant(inputs[2].(string), dt); e != nil {
		return &d.FuncReturn{Err: e}
	}

	return &d.FuncReturn{Value: sql, Output: dt}
}

func cast(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "cast", Inputs: []d.DataTypes{d.DTany, d.DTstring, d.DTany}, Output: d.DTany}
	}

	dialect := inputs[0].(*d.Dialect)
	col := inputs[2].(d.Column)
	var toDT d.DataTypes
	if toDT = d.DTFromString(inputs[1].(string)); toDT == d.DTunknown {
		return &d.FuncReturn{Err: fmt.Errorf("unknown data type %s", inputs[1].(string))}
	}
	var (
		sql string
		e   error
	)
	if sql, e = dialect.CastField(col.Name(""), toDT); e != nil {
		return &d.FuncReturn{Err: e}
	}

	return &d.FuncReturn{Value: sql, Output: toDT}
}

func exp(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "exp", Inputs: []d.DataTypes{d.DTany, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("exp(%s)", inputs[1].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func log(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "log", Inputs: []d.DataTypes{d.DTany, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("log(%s)", inputs[1].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}
