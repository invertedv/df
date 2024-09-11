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
			if !d.Compatible(info.Inputs[ind], cx.DataType(), false) {
				return nil, fmt.Errorf("in function %s: want data type %v got %v", info.Name, info.Inputs[ind], cx.DataType())
			}
			xs = append(xs, cx)
			continue
		}

		// check if value can be cast to correct type
		if _, e := d.ToDataType(inputs[ind], info.Inputs[ind], true); e != nil {
			return nil, e
		}

		// the functions expect all inputs to be *SQLcol.
		col := &SQLcol{sql: inputs[ind].(string), dType: info.Inputs[ind]}
		xs = append(xs, col)
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
	return d.RowFns{
		abs, add, and, cast, divide,
		eq, exp, ge, gt, ifs, le, log, lt,
		multiply, ne, not, or, subtract,
		toDate, toFloat, toInt, toString}
}

// ////////  Standard RowFns

func ifs(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "if", Inputs: []d.DataTypes{d.DTint, d.DTany, d.DTany}}
	}

	sqls := getData(inputs...)
	dts := getDataTypes(inputs...)
	if !d.Compatible(dts[1], dts[2], false) {
		return &d.RowFnReturn{Err: fmt.Errorf("incompatible data types in if")}
	}
	// TODO: make datatype compatibility check in sql.go

	sql := fmt.Sprintf("if(%s,%s,%s)", sqls[0], sqls[1], sqls[2])

	return &d.RowFnReturn{Value: sql, Output: dts[1]}
}

func abs(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", getData(inputs...)[0])
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func add(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTany}
	}

	sqls := getData(inputs...)
	dts := getDataTypes(inputs...)

	sql := fmt.Sprintf("%s + %s", sqls[0], sqls[1])
	var dtOut d.DataTypes
	dtOut = d.DTfloat
	if dts[0] == d.DTint && dts[1] == d.DTint {
		dtOut = d.DTint
	}
	return &d.RowFnReturn{Value: sql, Output: dtOut, Err: nil}
}

func and(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "and", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	sqls := getData(inputs...)
	sql := fmt.Sprintf("(%s and %s)", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func cast(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}
	sqls := getData(inputs...)

	var toDT d.DataTypes
	if toDT = d.DTFromString(sqls[0]); toDT == d.DTunknown {
		return &d.RowFnReturn{Err: fmt.Errorf("unknown data type %s", inputs[0].(string))}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[1], toDT); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: toDT}
}

func divide(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "divide", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s / %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func eq(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "eq", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s = %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func exp(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("exp(%s)", sqls[0])

	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func ge(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "ge", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s >= %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func gt(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "gt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s > %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func le(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "le", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s <= %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func log(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("log(%s)", sqls[0])
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func lt(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "lt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s < %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func multiply(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s * %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func ne(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "ne", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s != %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func not(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "not", Inputs: []d.DataTypes{d.DTint}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("(not %s)", sqls[0])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func or(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "or", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("(%s or %s)", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s - %s", sqls[0], sqls[1])
	return &d.RowFnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func toDate(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "date", Inputs: []d.DataTypes{d.DTany}, Output: d.DTdate}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTdate); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTdate}
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "float", Inputs: []d.DataTypes{d.DTany}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTfloat); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTfloat}
}

func toInt(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "int", Inputs: []d.DataTypes{d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTint); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTint}
}

func toString(info bool, context *d.Context, inputs ...any) *d.RowFnReturn {
	if info {
		return &d.RowFnReturn{Name: "string", Inputs: []d.DataTypes{d.DTany}, Output: d.DTstring}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTstring); e != nil {
		return &d.RowFnReturn{Err: e}
	}

	return &d.RowFnReturn{Value: sql, Output: d.DTstring}
}

////////////////////////

func getData(inputs ...any) []string {
	var sOut []string
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, inputs[ind].(*SQLcol).Data().(string))
	}

	return sOut
}

func getDataTypes(inputs ...any) []d.DataTypes {
	var sOut []d.DataTypes
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, inputs[ind].(*SQLcol).DataType())
	}

	return sOut
}
