package df

import (
	"fmt"
	"strings"
)

var SQLfunctions = LoadFunctions(false)

func SQLrun(fn *Func, params []any, inputs ...Column) (outCol Column, err error) {
	if len(inputs)+len(params) != len(fn.inputs) {
		return nil, fmt.Errorf("expected %d arguements to %s, got %d", len(inputs), fn.name, len(fn.inputs))
	}

	var (
		fnStr any
		dt    DataTypes
	)

	if fnStr, dt, err = fn.function(nil); err != nil {
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

	for ind := 0; ind < len(inputs); ind++ {
		if fn.inputs[ind+len(params)] != DTany && inputs[ind].DataType() != fn.inputs[ind+len(params)] {
			return nil, fmt.Errorf("column %s is data type %d, need %d", inputs[ind].Name(""), inputs[ind].DataType(), fn.inputs[ind+len(params)])
		}

		fnx = strings.Replace(fnx, fmt.Sprintf("X%d", ind), inputs[ind].Name(""), 1)
	}

	outCol = &SQLcol{
		name:   "",
		n:      1,
		dType:  dt,
		sql:    fnx,
		catMap: nil,
	}

	return outCol, nil
}

func sqlExp(inputs ...any) (any, DataTypes, error) {
	return "exp(X0)", DTfloat, nil
}

func sqlAbs(inputs ...any) (any, DataTypes, error) {
	return "abs(X0)", DTfloat, nil
}

func sqlAdd(inputs ...any) (any, DataTypes, error) {
	return "X0+X1", DTfloat, nil
}

func sqlCast(inputs ...any) (any, DataTypes, error) {
	switch inputs[0].(string) {
	case "DTfloat":
		return "cast(X0 AS Float64)", DTfloat, nil
	case "DTint":
		return "cast(X0 AS Int64)", DTint, nil
	case "DTstring":
		return "cast(X0 AS String)", DTstring, nil
	case "DTdate":
		return "cast(cast(X0 AS String) AS Date", DTdate, nil
	}

	return nil, DTunknown, fmt.Errorf("cannot cast to %s", inputs[0].(string))
}
