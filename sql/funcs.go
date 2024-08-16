package sql

import (
	"fmt"

	d "github.com/invertedv/df"
)

func Run(fn d.AnyFunction, params []any, inputs []d.Column) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs)+len(params) != len(info.Inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), info.Name, len(info.Inputs))
	}

	var xs []any
	for ind := 0; ind < len(params); ind++ {
		var (
			xadd any
			e    error
		)

		if xadd, e = d.ToDataType(params[ind], info.Inputs[ind], true); e != nil {
			return nil, e
		}
		xs = append(xs, xadd)
	}

	for ind := 0; ind < len(inputs); ind++ {
		if info.Inputs[ind+len(params)] != d.DTany && inputs[ind].DataType() != info.Inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", inputs[ind].Name(""), inputs[ind].DataType(), info.Inputs[ind+len(params)])
		}
		xs = append(xs, inputs[ind])
	}

	r := fn(false, xs...)

	outCol = &SQLcol{
		name:   "",
		dType:  r.Output,
		sql:    r.Value.(string),
		catMap: nil,
	}

	return outCol, nil
}

func StandardFunctions() d.Functions {
	return d.Functions{exp, abs, cast, add}

}

// ////////  Standard Functions
func exp(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", inputs[0].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
	return &d.FuncReturn{Value: "exp(X0)", Err: nil}
}

func abs(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", inputs[0].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func add(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("%s + %s", inputs[0].(d.Column).Name(""), inputs[1].(d.Column).Name(""))
	return &d.FuncReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func cast(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}

	x := inputs[1].(d.Column).Name("")
	sql := fmt.Sprintf("cast(%s AS", x)

	switch inputs[0].(string) {
	case "DTfloat":
		return &d.FuncReturn{Value: sql + " Float64)", Output: d.DTfloat, Err: nil}
	case "DTint":
		return &d.FuncReturn{Value: sql + " Int64)", Output: d.DTint, Err: nil}
	case "DTstring":
		return &d.FuncReturn{Value: sql + " String)", Output: d.DTstring, Err: nil}
	case "DTdate":
		return &d.FuncReturn{Value: "cast(" + sql + " String) AS Date", Output: d.DTdate, Err: nil}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot cast to %s", inputs[0].(string))}
}
