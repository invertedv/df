package sql

import (
	"fmt"

	d "github.com/invertedv/df"
	m "github.com/invertedv/df/mem"
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

	skip := false
	for j := 0; j < len(inputs); j++ {
		var (
			ok  bool
			col *SQLcol
		)

		if col, ok = inputs[j].(*SQLcol); !ok {
			var e error
			sig := context.Self().(*SQLdf).Signature()
			ver := context.Self().(*SQLdf).Version()
			if col, e = NewColScalar("", sig, ver, inputs[j]); e != nil {
				return nil, e
			}
		}

		// if this is an *SQLcol with no scalarValue, then it's a true column from the data frame and there's no point
		// to trying to evaluate it as a scalar
		if ok && col.scalarValue == nil {
			skip = true
		}

		inps = append(inps, col)
		cols = append(cols, col)
	}

	if okx, _ := okParams(cols, info.Inputs, info.Output); !okx {
		return nil, fmt.Errorf("bad parameters to %s", info.Name)
	}

	// if scalars are available for all the inputs, then process them as a scalar formula
	if !skip {
		// get the corresponding mem function
		if fnMem := m.StandardFunctions().Get(info.Name); fnMem != nil {
			var (
				col   *m.MemCol
				e     error
				inpsx []any
			)

			// Create *MemCol version of the inputs
			for j := 0; j < len(inputs); j++ {
				v := inputs[j]
				if cols[j].scalarValue != nil {
					v = cols[j].scalarValue
				}
				if col, e = m.NewMemCol("", v); e != nil {
					return nil, e
				}
				inpsx = append(inpsx, col)
			}

			// run the function, convert output to SQLcol
			if val := fnMem(false, context, inpsx...); val.Err == nil {
				mCol := val.Value.(*m.MemCol)
				dt := mCol.DataType()
				sql, _ := context.Dialect().CastField(d.Any2String(mCol.Element(0)), dt, dt)
				sig := context.Self().(*SQLdf).Signature()
				ver := context.Self().(*SQLdf).Version()
				src := context.Self().(*SQLdf).MakeQuery()
				retCol := NewColSQL("", sig, src, ver, context.Dialect(), mCol.DataType(), sql)
				// Place the value of the output in .scalarValue in case that's needed later
				retCol.scalarValue = mCol.Element(0)
				return retCol, nil
			}
		}
	}

	var fnR *d.FnReturn
	if fnR = fn(false, context, inps...); fnR.Err != nil {
		return nil, fnR.Err
	}

	//TODO: check return type

	return fnR.Value, nil
}

func StandardFunctions() d.Fns {
	return d.Fns{abs, add, and, applyCat, divide, eq, exp, ge, gt, ifs, le, log, lt, mean,
		multiply, ne, not, or, sortDF, sum, subtract, table, toCat, toDate, toFloat, toInt, toString, where}
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

func table(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "table", Inputs: [][]d.DataTypes{{d.DTint}, {d.DTstring}, {d.DTdate}},
			Output: []d.DataTypes{d.DTdf, d.DTdf, d.DTdf}, Varying: true}
	}
	var (
		outDF d.DF
		e     error
	)

	var names []string
	for ind := 0; ind < len(inputs); ind++ {
		names = append(names, inputs[ind].(*SQLcol).Name(""))
	}

	if outDF, e = context.Self().(*SQLdf).Table(false, names...); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outDF}
}

func sortDF(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "sort", Inputs: [][]d.DataTypes{{d.DTstring}},
			Output: []d.DataTypes{d.DTdf}, Varying: true}
	}

	ascending := true
	// Any2String will strip out the single quotes
	if d.Any2String(inputs[0].(*SQLcol).Data()) == "desc" {
		ascending = false
	}

	var (
		colNames []string
		e        error
	)

	if colNames, e = getNames(1, inputs...); e != nil {
		return &d.FnReturn{Err: e}
	}

	if ex := context.Self().Sort(ascending, colNames...); ex != nil {
		return &d.FnReturn{Err: ex}
	}

	return &d.FnReturn{Value: context.Self()}
}

// getNames returns the names of the input Columns starting with startInd element
func getNames(startInd int, cols ...any) ([]string, error) {
	var colNames []string
	for ind := startInd; ind < len(cols); ind++ {
		var cn string
		if cn = cols[ind].(*SQLcol).Name(""); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

// ***************** categorical Operations *****************

func toCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*SQLcol)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	fuzz := 1
	if len(inputs) > 1 {
		f := inputs[1].(*SQLcol).Data()

		var (
			ex error
			fa any
		)
		if fa, ex = d.ToDataType(f, d.DTint, true); ex != nil {
			return &d.FnReturn{Err: fmt.Errorf("cannot interpret fuzz as integer in cat")}
		}

		fuzz = fa.(int)
		if fuzz < 1 {
			return &d.FnReturn{Err: fmt.Errorf("fuzz value must be positive")}
		}
	}

	var (
		outCol d.Column
		e      error
	)

	if outCol, e = context.Self().(*SQLdf).Categorical(col.Name(""), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

func applyCat(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := inputs[0].(*SQLcol)
	oldData := inputs[1].(*SQLcol)
	newVal := inputs[2].(*SQLcol)

	if newData.DataType() != oldData.rawType {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	var (
		defaultValue any
		e            error
	)

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	if defaultValue, e = d.ToDataType(newVal.scalarValue, newVal.DataType(), true); e != nil {
		return &d.FnReturn{Err: e}
	}

	var levels []any
	for k := range oldData.catMap {
		levels = append(levels, k)
	}

	var outCol d.Column
	if outCol, e = context.Self().(*SQLdf).Categorical(newData.Name(""), oldData.catMap, 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol.(*SQLcol).rawType = newData.DataType()
	outFn := &d.FnReturn{Value: outCol}

	return outFn
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

	// The parentheses are required based on how the parser works.
	sql := fmt.Sprintf("(%s %s %s)", sqls[0], op, sqls[1])
	// handles cases like x--3
	if sqls[0] == "'zero'" {
		sql = "-" + sqls[1]
	}
	var dtOut d.DataTypes
	dtOut = d.DTint

	if dts[0] == d.DTfloat || dts[1] == d.DTfloat {
		dtOut = d.DTfloat
	}

	tabl := context.Self().(*SQLdf).Signature()
	source := context.Self().(*SQLdf).MakeQuery()
	version := context.Self().(*SQLdf).Version()

	//sql, _ = context.Dialect().CastField(sql, dtOut, dtOut)
	outCol := NewColSQL("", tabl, source, version, context.Dialect(), dtOut, sql)

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

func gt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("gt", "(%s > %s)", "", inps, outp, info, context, inputs...)
}

func ge(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("ge", "(%s >= %s)", "", inps, outp, info, context, inputs...)
}

func lt(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("lt", "(%s < %s)", "", inps, outp, info, context, inputs...)
}

func le(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("le", "(%s <= %s)", "", inps, outp, info, context, inputs...)
}

func eq(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("eq", "(%s == %s)", "", inps, outp, info, context, inputs...)
}

func ne(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("ne", "(%s != %s)", "", inps, outp, info, context, inputs...)
}

func and(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("and", "and(%s,%s)", "", inps, outp, info, context, inputs...)
}

func or(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("or", "or(%s, %s)", "", inps, outp, info, context, inputs...)
}

func not(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("not", "not(%s)", "", inps, outp, info, context, inputs...)
}

func ifs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat},
		{d.DTint, d.DTint, d.DTint}, {d.DTint, d.DTdate, d.DTdate}, {d.DTint, d.DTstring, d.DTstring}}
	outp := []d.DataTypes{d.DTfloat, d.DTint, d.DTdate, d.DTstring}
	return fnGen("if", "if(%s>0,%s,%s)", "", inp, outp, info, context, inputs...)
}

// ***************** math operations *****************

func exp(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return fnGen("exp", "exp(%s)", "", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, context, inputs...)
}

func log(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return fnGen("log", "log(%s)", "", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, context, inputs...)
}

func abs(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	return fnGen("abs", "abs(%s)", "", [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint},
		info, context, inputs...)
}

// ***************** type conversions *****************
func cast(name string, out d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTdate}, {d.DTstring}, {d.DTcategorical}},
			Output: []d.DataTypes{out, out, out, out, out}}
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

	sig := context.Self().(*SQLdf).Signature()
	ver := context.Self().(*SQLdf).Version()
	source := context.Self().(*SQLdf).MakeQuery()
	dlct := context.Dialect()
	outCol := NewColSQL("", sig, source, ver, dlct, out, sql)

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

// ***************** Functions that return a scalar *****************

func sum(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint}, {d.DTfloat}}
	outp := []d.DataTypes{d.DTint, d.DTfloat}
	return fnGen("sum", "sum(%s)", "S", inp, outp, info, context, inputs...)
}

func mean(info bool, context *d.Context, inputs ...any) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint}, {d.DTfloat}}
	outp := []d.DataTypes{d.DTfloat, d.DTfloat}
	return fnGen("mean", "avg(%s)", "S", inp, outp, info, context, inputs...)
}

// ***************** Helpers *****************

func getSQL(inputs ...any) []string {
	var sOut []string
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, inputs[ind].(*SQLcol).Data().(string)) // HERE
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

func fnGen(name, sql, suffix string, inp [][]d.DataTypes, outp []d.DataTypes, info bool, context *d.Context, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
	}

	sqls := getSQL(inputs...)
	dts := getDataTypes(inputs...)

	var sa []any
	for j := 0; j < len(sqls); j++ {
		sa = append(sa, sqls[j])
	}

	sqlOut := fmt.Sprintf(sql, sa...)

	var outType d.DataTypes
	// output type
	for ind := 0; ind < len(inp); ind++ {
		ok := true
		for j := 0; j < len(dts); j++ {
			if dts[j] != inp[ind][j] {
				ok = false
				break
			}
		}

		if ok {
			outType = outp[ind]
			break
		}
	}

	sig := context.Self().(*SQLdf).Signature() + suffix
	ver := context.Self().(*SQLdf).Version()
	source := context.Self().(*SQLdf).MakeQuery()
	//	sqlOut, _ = context.Dialect().CastField(sqlOut, outType, outType)

	outCol := NewColSQL("", sig, source, ver, context.Dialect(), outType, sqlOut)

	return &d.FnReturn{Value: outCol}
}
