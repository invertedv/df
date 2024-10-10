package sql

import (
	"fmt"

	d "github.com/invertedv/df"
)

func RunDFfn(fn d.Fn, context *d.Context, inputs []any) (any, error) {
	info := fn(true, nil)
	if !info.Varying && len(inputs) != len(info.Inputs[0]) {
		return nil, fmt.Errorf("got %d arguments to %s, expected %d", len(inputs), info.Name, len(info.Inputs))
	}

	if info.Varying && len(inputs) < len(info.Inputs[0]) {
		return nil, fmt.Errorf("need at least %d arguments to %s", len(inputs), info.Name)
	}

	var (
		inps []any
		cols []*SQLcol
	)
	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col *SQLcol
		)
		if col, ok = inputs[j].(*SQLcol); !ok {
			var e error
			table := context.Self().(*SQLdf).MakeQuery()
			if col, e = NewColScalar("", table, inputs[j]); e != nil {
				return nil, e
			}
		}

		inps = append(inps, col)
		cols = append(cols, col)
	}

	if ok, _ := okParams(cols, info.Inputs, info.Output); !ok {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	var fnR *d.FnReturn
	if fnR = fn(false, context, inps...); fnR.Err != nil {
		return nil, fnR.Err
	}

	//TODO: check return type

	return fnR.Value, nil
}

func StandardFunctions() d.Fns {

	return d.Fns{abs, add, and, divide, eq, exp, ge, gt, le, log, lt, multiply, ne, not, or, subtract, toDate, toFloat, toInt, toString, where}
	//	return d.Fns{
	//		abs, add, and, cast, divide,
	//		eq, exp, ge, gt, ifs, le, log, lt,
	//		multiply, ne, not, or, subtract,
	//		toDate, toFloat, toInt, toString}
}

// ////////  Standard Fns

// ***************** Functions that return a data frame *****************

func where(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "where", Inputs: [][]d.DataTypes{{d.DTint}}, Output: []d.DataTypes{d.DTdf}}
	}

	var (
		outDF d.DF
		e     error
	)
	outDF, e = context.Self().Where(inputs[0].(d.Column))

	return &d.FnReturn{Value: outDF, Err: e}
}

// ***************** arithmetic operations *****************

func arithmetic(op, name string, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat},
			{d.DTint, d.DTint}, {d.DTstring, d.DTfloat}, {d.DTstring, d.DTint}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTfloat, d.DTint}}
	}
	sqls := getSQL(inputs...)
	dts := getDataTypes(inputs...)

	// handles cases like x--3
	if sqls[0] == "'zero'" {
		sqls[0] = "0"
	}
	// The parentheses are required based on how the parser works.
	sql := fmt.Sprintf("(%s %s %s)", sqls[0], op, sqls[1])
	var dtOut d.DataTypes
	dtOut = d.DTint

	if dts[0] == d.DTfloat || dts[1] == d.DTfloat {
		dtOut = d.DTfloat
	}

	table := context.Self().(*SQLdf).MakeQuery()

	outCol := NewColSQL("", table, dtOut, sql)

	return &d.FnReturn{Value: outCol}
}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("+", "add", info, context, inputs...)
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("-", "subtract", info, context, inputs...)
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("*", "multiply", info, context, inputs...)
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return arithmetic("/", "divide", info, context, inputs...)
}

// ***************** logical operations *****************

func prep(op, name string, inps [][]d.DataTypes, outp []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: inps, Output: outp}
	}
	sqls := getSQL(inputs...)

	// The parentheses are required based on how the parser works.
	sql := fmt.Sprintf("(%s %s %s)", sqls[0], op, sqls[1])

	table := context.Self().(*SQLdf).MakeQuery()

	outCol := NewColSQL("", table, d.DTint, sql)

	return &d.FnReturn{Value: outCol}
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep(">", "gt", inps, outp, info, context, inputs...)
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep(">=", "ge", inps, outp, info, context, inputs...)
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep("<", "lt", inps, outp, info, context, inputs...)
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep("<=", "le", inps, outp, info, context, inputs...)
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep("==", "eq", inps, outp, info, context, inputs...)
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return prep("!=", "ne", inps, outp, info, context, inputs...)
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return prep("and", "and", inps, outp, info, context, inputs...)
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return prep("or", "or", inps, outp, info, context, inputs...)
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return singleFn("not", inps, outp, info, context, inputs...)
}

// real functions that take a single argument
func singleFn(name string, inp [][]d.DataTypes, outp []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
	}

	colSQL := inputs[0].(*SQLcol).Data().(string)
	colDt := inputs[0].(*SQLcol).DataType()

	sql := fmt.Sprintf("%s(%s)", name, colSQL)

	// what datatype is the output?
	var dtOut d.DataTypes
	for ind := 0; ind < len(inp); ind++ {
		if colDt == inp[ind][0] {
			dtOut = outp[ind]
		}
	}

	table := context.Self().(*SQLdf).MakeQuery()

	outCol := NewColSQL("", table, dtOut, sql)

	return &d.FnReturn{Value: outCol}
}

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return singleFn("exp", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, context, inputs...)
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return singleFn("log", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, context, inputs...)
}

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return singleFn("abs", [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint}, info, context, inputs...)
}

// ***************** type conversions *****************
func cast(name string, out d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTdate}, {d.DTstring}},
			Output: []d.DataTypes{out, out, out, out}}
	}

	inp := inputs[0].(*SQLcol).Data().(string)
	dt := inputs[0].(*SQLcol).DataType()

	var (
		sql string
		e   error
	)

	if sql, e = context.Dialect().CastField(inp, dt, out); e != nil {
		return &d.FnReturn{Err: e}
	}

	/*	if _, ex := context.Self().Column(inp); ex != nil {
			if sql, e = context.Dialect().CastConstant(inp, out); e != nil {
				return &d.FnReturn{Err: e}
			}
		} else {
			if sql, e = context.Dialect().CastField(inp, out); e != nil {
				return &d.FnReturn{Err: e}
			}
		}

	*/

	table := context.Self().(*SQLdf).MakeQuery()
	outCol := NewColSQL("", table, out, sql)
	return &d.FnReturn{Value: outCol}

}

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return cast("float", d.DTfloat, info, context, inputs...)
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return cast("int", d.DTint, info, context, inputs...)
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return cast("date", d.DTdate, info, context, inputs...)
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return cast("string", d.DTstring, info, context, inputs...)
}

/*
func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "if", Inputs: []d.DataTypes{d.DTint, d.DTany, d.DTany}}
	}

	sqls := getData(inputs...)
	dts := getDataTypes(inputs...)
	if !d.Compatible(dts[1], dts[2], false) {
		return &d.FnReturn{Err: fmt.Errorf("incompatible data types in if")}
	}
	// TODO: make datatype compatibility check in sql.go

	sql := fmt.Sprintf("if(%s,%s,%s)", sqls[0], sqls[1], sqls[2])

	return &d.FnReturn{Value: sql, Output: dts[1]}
}

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "abs", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}

	sql := fmt.Sprintf("abs(%s)", getData(inputs...)[0])
	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func add(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTany}
	}

	sqls := getData(inputs...)
	dts := getDataTypes(inputs...)

	sql := fmt.Sprintf("%s + %s", sqls[0], sqls[1])
	var dtOut d.DataTypes
	dtOut = d.DTfloat
	if dts[0] == d.DTint && dts[1] == d.DTint {
		dtOut = d.DTint
	}
	return &d.FnReturn{Value: sql, Output: dtOut, Err: nil}
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "and", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}

	sqls := getData(inputs...)
	sql := fmt.Sprintf("(%s and %s)", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func cast(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cast", Inputs: []d.DataTypes{d.DTstring, d.DTany}, Output: d.DTany}
	}
	sqls := getData(inputs...)

	var toDT d.DataTypes
	if toDT = d.DTFromString(sqls[0]); toDT == d.DTunknown {
		return &d.FnReturn{Err: fmt.Errorf("unknown data type %s", inputs[0].(string))}
	}

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[1], toDT); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: sql, Output: toDT}
}

func divide(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "divide", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s / %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "eq", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s = %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "exp", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("exp(%s)", sqls[0])

	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ge", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s >= %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "gt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s > %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "le", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s <= %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "log", Inputs: []d.DataTypes{d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("log(%s)", sqls[0])
	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "lt", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s < %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func multiply(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "multiply", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s * %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "ne", Inputs: []d.DataTypes{d.DTany, d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s != %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "not", Inputs: []d.DataTypes{d.DTint}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("(not %s)", sqls[0])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "or", Inputs: []d.DataTypes{d.DTint, d.DTint}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("(%s or %s)", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func subtract(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "add", Inputs: []d.DataTypes{d.DTfloat, d.DTfloat}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	sql := fmt.Sprintf("%s - %s", sqls[0], sqls[1])
	return &d.FnReturn{Value: sql, Output: d.DTfloat, Err: nil}
}

func toDate(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "date", Inputs: []d.DataTypes{d.DTany}, Output: d.DTdate}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTdate); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: sql, Output: d.DTdate}
}

func toFloat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "float", Inputs: []d.DataTypes{d.DTany}, Output: d.DTfloat}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTfloat); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: sql, Output: d.DTfloat}
}

func toInt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "int", Inputs: []d.DataTypes{d.DTany}, Output: d.DTint}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTint); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: sql, Output: d.DTint}
}

func toString(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "string", Inputs: []d.DataTypes{d.DTany}, Output: d.DTstring}
	}
	sqls := getData(inputs...)

	var (
		sql string
		e   error
	)
	if sql, e = context.Dialect().CastField(sqls[0], d.DTstring); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: sql, Output: d.DTstring}
}
*/
////////////////////////

func getSQL(inputs ...any) []string {
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

func okParams(cols []*SQLcol, inputs [][]d.DataTypes, outputs []d.DataTypes) (ok bool, outType d.DataTypes) {
	for j := 0; j < len(inputs); j++ {
		ok = true
		for k := 0; k < len(inputs[j]); k++ {
			if cols[k].DataType() != inputs[j][k] {
				ok = false
				break
			}
		}

		if ok {
			return true, outputs[j]
		}
	}

	return false, d.DTunknown
}
