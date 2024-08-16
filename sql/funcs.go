package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
)

func Run(fn d.AnyFunction, params []any, inputs []d.Column) (outCol d.Column, err error) {
	info := fn(true, nil)
	if len(inputs)+len(params) != len(info.Inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), info.Name, len(info.Inputs))
	}

	var fnr *d.FuncReturn
	if fnr = fn(false, ""); fnr.Err != nil {
		return nil, fnr.Err
	}

	fnx := fnr.Value.(string)
	for ind := 0; ind < len(params); ind++ {
		var (
			xadd any
			e    error
		)

		if xadd, e = d.ToDataType(params[ind], info.Inputs[ind], true); e != nil {
			return nil, e
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("P%d", ind), fmt.Sprintf("%d", xadd), 1)
	}

	for ind := 0; ind < len(inputs); ind++ {
		if info.Inputs[ind+len(params)] != d.DTany && inputs[ind].DataType() != info.Inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", inputs[ind].Name(""), inputs[ind].DataType(), info.Inputs[ind+len(params)])
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), inputs[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		dType:  info.Output,
		sql:    fnx,
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

	return &d.FuncReturn{Value: "exp(X0)", Err: nil}
}

func abs(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FuncReturn{Value: "abs(X0)", Err: nil}
}

func add(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}

	return &d.FuncReturn{Value: "X0+X1", Err: nil}
}

func cast(info bool, inputs ...any) *d.FuncReturn {
	if info {
		return &d.FuncReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}}
	}

	switch inputs[0].(string) {
	case "DTfloat":
		return &d.FuncReturn{Value: "cast(X0 AS Float64)", Err: nil}
	case "DTint":
		return &d.FuncReturn{Value: "cast(X0 AS Int64)", Err: nil}
	case "DTstring":
		return &d.FuncReturn{Value: "cast(X0 AS String)", Err: nil}
	case "DTdate":
		return &d.FuncReturn{Value: "cast(cast(X0 AS String) AS Date", Err: nil}
	}

	return &d.FuncReturn{Value: nil, Err: fmt.Errorf("cannot cast to %s", inputs[0].(string))}
}
