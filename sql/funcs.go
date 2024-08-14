package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
	u "github.com/invertedv/utilities"
)

var Functions = d.FunctionList{exp, abs, cast, add}

func Functionsx(funcName string) d.AnyFunction {
	fns := []d.AnyFunction{
		exp, abs, cast, add,
	}

	var names []string

	for ind := 0; ind < len(fns); ind++ {
		fnr := fns[ind](nil)
		names = append(names, fnr.Name)
	}

	pos := u.Position(funcName, "", names...)
	if pos < 0 {
		return nil
	}

	return fns[pos]
}

func Run(fn *d.Func, params []any, inputs []d.Column) (outCol d.Column, err error) {
	if len(inputs)+len(params) != len(fn.Inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.Name, len(fn.Inputs))
	}

	var fnr *d.FuncReturn
	if fnr = fn.Function(""); fnr.Err != nil {
		return nil, fnr.Err
	}

	fnx := fnr.Value.(string)
	for ind := 0; ind < len(params); ind++ {
		var (
			xadd any
			e    error
		)

		if xadd, e = d.ToDataType(params[ind], fn.Inputs[ind], true); e != nil {
			return nil, e
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("P%d", ind), fmt.Sprintf("%d", xadd), 1)
	}

	for ind := 0; ind < len(inputs); ind++ {
		if fn.Inputs[ind+len(params)] != d.DTany && inputs[ind].DataType() != fn.Inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", inputs[ind].Name(""), inputs[ind].DataType(), fn.Inputs[ind+len(params)])
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), inputs[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		n:      1,
		dType:  fnr.DT,
		sql:    fnx,
		catMap: nil,
	}

	return outCol, nil
}

func exp(inputs ...any) *d.FuncReturn {
	myName := "exp"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	return &d.FuncReturn{Value: "exp(X0)", DT: d.DTfloat, Name: myName, Err: nil}
}

func abs(inputs ...any) *d.FuncReturn {
	myName := "abs"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	return &d.FuncReturn{Value: "abs(X0)", DT: d.DTfloat, Name: myName, Err: nil}
}

func add(inputs ...any) *d.FuncReturn {
	myName := "add"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	return &d.FuncReturn{Value: "X0+X1", DT: d.DTfloat, Name: myName, Err: nil}
}

func cast(inputs ...any) *d.FuncReturn {
	myName := "cast"
	if inputs[0] == nil {
		return &d.FuncReturn{Name: myName}
	}

	switch inputs[0].(string) {
	case "DTfloat":
		return &d.FuncReturn{Value: "cast(X0 AS Float64)", DT: d.DTfloat, Name: myName, Err: nil}
	case "DTint":
		return &d.FuncReturn{Value: "cast(X0 AS Int64)", DT: d.DTint, Name: myName, Err: nil}
	case "DTstring":
		return &d.FuncReturn{Value: "cast(X0 AS String)", DT: d.DTstring, Name: myName, Err: nil}
	case "DTdate":
		return &d.FuncReturn{Value: "cast(cast(X0 AS String) AS Date", DT: d.DTdate, Name: myName, Err: nil}
	}

	return &d.FuncReturn{Value: nil, DT: d.DTunknown, Name: myName, Err: fmt.Errorf("cannot cast to %s", inputs[0].(string))}
}
