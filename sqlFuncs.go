package df

import (
	"fmt"
	"strings"
)

type SQLfunc struct {
	name     string
	inputs   []DataTypes
	output   DataTypes
	function AnyFunction
}

type SQLfuncMap map[string]*SQLfunc

var SQLfunctions = sqlLoadFunctions()

func (fn *SQLfunc) Run(inputs ...any) (outCol Column, err error) {
	if len(inputs) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		vals   []*SQLcol
		params []any
	)

	for ind := 0; ind < len(inputs); ind++ {
		var (
			col *SQLcol
			ok  bool
		)

		if col, ok = inputs[ind].(*SQLcol); ok {
			vals = append(vals, col)
		} else {
			params = append(params, inputs[ind])
		}
	}

	var fnStr any
	if fnStr, err = fn.function(inputs...); err != nil {
		return nil, err
	}

	fnx := fnStr.(string)
	for ind := 0; ind < len(params); ind++ {
		xadd, e := toDataType(params[ind], fn.inputs[ind], true)
		if e != nil {
			return nil, e
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("P%d", ind), fmt.Sprintf("%d", xadd), 1)
	}

	for ind := 0; ind < len(vals); ind++ {
		if fn.inputs[ind+len(params)] != DTany && vals[ind].DataType() != fn.inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", vals[ind].Name(""), vals[ind].DataType(), fn.inputs[ind+len(params)])
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), vals[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		n:      1,
		dType:  fn.output,
		sql:    fnx,
		catMap: nil,
	}

	return outCol, nil
}

func sqlLoadFunctions() SQLfuncMap {
	fns := make(SQLfuncMap)
	names, inputs, outputs, fnsx := funcDetails(false)
	for ind := 0; ind < len(names); ind++ {
		fns[names[ind]] = &SQLfunc{
			name:     names[ind],
			inputs:   inputs[ind],
			output:   outputs[ind],
			function: fnsx[ind],
		}
	}

	return fns
}

func sqlExp(inputs ...any) (any, error) {
	return "exp(X0)", nil
}

func sqlAbs(inputs ...any) (any, error) {
	return "abs(X0)", nil
}

func sqlAdd(inputs ...any) (any, error) {
	return "X0+X1", nil
}

func sqlCast(inputs ...any) (any, error) {
	switch inputs[0].(string) {
	case "DTfloat":
		return "cast(X0 AS Float64)", nil
	case "DTint":
		return "cast(X0 AS Int64)", nil
	case "DTstring":
		return "cast(X0 AS String)", nil
	case "DTdate":
		return "cast(cast(X0 AS String) AS Date", nil
	}

	return nil, fmt.Errorf("cannot cast to %s", inputs[0].(string))
}
