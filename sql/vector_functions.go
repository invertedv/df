package sql

import (
	"fmt"

	d "github.com/invertedv/df"
)

// ////////  Standard Fns

// getNames returns the names of the input Columns starting with startInd element
func getNames(startInd int, cols ...d.Column) ([]string, error) {
	var colNames []string
	for ind := startInd; ind < len(cols); ind++ {
		var cn string
		if cn = cols[ind].(*Col).Name(); cn == "" {
			return nil, fmt.Errorf("column with no name in table")
		}

		colNames = append(colNames, cn)
	}

	return colNames, nil
}

// ***************** Functions that take no parameters *****************

func rowNumberX(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return fnGen("rowNumber", "", nil, []d.DataTypes{d.DTint}, info, df)
	}
	sqlx := df.Dialect().RowNumber()
	//	return fnGen("gt", "(%s > %s)", "", inps, outp, info, context, inputs...)
	return fnGen("rowNumber", sqlx, nil, []d.DataTypes{d.DTint}, info, df)
}

// ***************** categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "cat", Inputs: [][]d.DataTypes{{d.DTstring}, {d.DTint}, {d.DTdate}},
			Output:  []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical},
			Varying: true}
	}

	col := inputs[0].(*Col)
	dt := col.DataType()
	if !(dt == d.DTint || dt == d.DTstring || dt == d.DTdate) {
		return &d.FnReturn{Err: fmt.Errorf("cannot make %s into categorical", dt)}
	}

	fuzz := 1
	if len(inputs) > 1 {
		f := toCol(df, inputs[1]).SQL()

		var (
			fa any
			ok bool
		)
		if fa, ok = d.ToDataType(f, d.DTint); !ok {
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

	if outCol, e = df.(*DF).Categorical(col.Name(), nil, fuzz, nil, nil); e != nil {
		return &d.FnReturn{Err: e}
	}

	return &d.FnReturn{Value: outCol}
}

func applyCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := inputs[0].(*Col)
	oldData := inputs[1].(*Col)
	newVal := inputs[2].(*Col)

	if newData.DataType() != oldData.RawType() {
		return &d.FnReturn{Err: fmt.Errorf("new column must be same type as original data in applyCat")}
	}

	if newVal.DataType() != newData.DataType() {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value to correct type in applyCat")}
	}

	var (
		defaultValue any
		ok           bool
	)
	if defaultValue, ok = d.ToDataType(newVal.SQL(), newVal.DataType()); !ok {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value")}
	}

	var levels []any
	for k := range oldData.CategoryMap() {
		levels = append(levels, k)
	}

	var (
		outCol d.Column
		e      error
	)
	if outCol, e = df.(*DF).Categorical(newData.Name(), oldData.CategoryMap(), 0, defaultValue, levels); e != nil {
		return &d.FnReturn{Err: e}
	}

	_ = d.ColRawType(newData.DataType())(outCol.(*Col).ColCore)
	outFn := &d.FnReturn{Value: outCol}

	return outFn
}

// ***************** arithmetic operations *****************

func arithmetic(op, name string, info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat, d.DTfloat},
			{d.DTint, d.DTint}, {d.DTstring, d.DTfloat}, {d.DTstring, d.DTint}},
			Output: []d.DataTypes{d.DTfloat, d.DTint, d.DTfloat, d.DTint}}
	}
	sqls := getSQL(df, inputs...)
	dts := getDataTypes(df, inputs...)

	// The parentheses are required based on how the parser works.
	sql := fmt.Sprintf("(%s %s %s)", sqls[0], op, sqls[1])
	var dtOut d.DataTypes
	dtOut = d.DTint

	if dts[0] == d.DTfloat || dts[1] == d.DTfloat {
		dtOut = d.DTfloat
	}

	outCol, _ := NewColSQL(dtOut, df.Dialect(), sql)

	return &d.FnReturn{Value: outCol}
}

func add(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return arithmetic("+", "add", info, df, inputs...)
}

func subtract(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return arithmetic("-", "subtract", info, df, inputs...)
}

func multiply(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return arithmetic("*", "multiply", info, df, inputs...)
}

func divide(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return arithmetic("/", "divide", info, df, inputs...)
}

// ***************** logical operations *****************

func gt(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("gt", "(%s > %s)", inps, outp, info, df, inputs...)
}

func ge(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("ge", "(%s >= %s)", inps, outp, info, df, inputs...)
}

func lt(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("lt", "(%s < %s)", inps, outp, info, df, inputs...)
}

func le(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("le", "(%s <= %s)", inps, outp, info, df, inputs...)
}

func eq(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("eq", "(%s == %s)", inps, outp, info, df, inputs...)
}

func ne(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTfloat, d.DTfloat}, {d.DTint, d.DTint}, {d.DTstring, d.DTstring}, {d.DTdate, d.DTdate}}
	outp := []d.DataTypes{d.DTint, d.DTint, d.DTint, d.DTint}
	return fnGen("ne", "(%s != %s)", inps, outp, info, df, inputs...)
}

func and(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("and", "and(%s,%s)", inps, outp, info, df, inputs...)
}

func or(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint, d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("or", "or(%s, %s)", inps, outp, info, df, inputs...)
}

func not(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inps := [][]d.DataTypes{{d.DTint}}
	outp := []d.DataTypes{d.DTint}
	return fnGen("not", "not(%s)", inps, outp, info, df, inputs...)
}

func ifs(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	inp := [][]d.DataTypes{{d.DTint, d.DTfloat, d.DTfloat},
		{d.DTint, d.DTint, d.DTint}, {d.DTint, d.DTdate, d.DTdate}, {d.DTint, d.DTstring, d.DTstring}}
	outp := []d.DataTypes{d.DTfloat, d.DTint, d.DTdate, d.DTstring}
	return fnGen("if", "if(%s>0,%s,%s)", inp, outp, info, df, inputs...)
}

// ***************** math operations *****************

func exp(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return fnGen("exp", "exp(%s)", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, df, inputs...)
}

func log(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return fnGen("log", "log(%s)", [][]d.DataTypes{{d.DTfloat}}, []d.DataTypes{d.DTfloat}, info, df, inputs...)
}

func abs(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return fnGen("abs", "abs(%s)", [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint},
		info, df, inputs...)
}

func neg(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return fnGen("neg", "-%s", [][]d.DataTypes{{d.DTfloat}, {d.DTint}}, []d.DataTypes{d.DTfloat, d.DTint}, info, df, inputs...)
}

// ***************** type conversions *****************
func cast(name string, out d.DataTypes, info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: [][]d.DataTypes{{d.DTfloat}, {d.DTint}, {d.DTdate}, {d.DTstring}, {d.DTcategorical}},
			Output: []d.DataTypes{out, out, out, out, out}}
	}
	// TODO: make a col var
	inp := toCol(df, inputs[0]).SQL()
	dt := toCol(df, inputs[0]).DataType()

	var (
		sql string
		e   error
	)

	if sql, e = df.Dialect().CastField(inp, dt, out); e != nil {
		return &d.FnReturn{Err: e}
	}

	outCol, _ := NewColSQL(out, df.Dialect(), sql)

	return &d.FnReturn{Value: outCol}
}

func toFloat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return cast("float", d.DTfloat, info, df, inputs...)
}

func toInt(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return cast("int", d.DTint, info, df, inputs...)
}

func toDate(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return cast("date", d.DTdate, info, df, inputs...)
}

func toString(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	return cast("string", d.DTstring, info, df, inputs...)
}

// ***************** Helpers *****************

func toCol(df d.DF, x any) *Col {
	if c, ok := x.(*Col); ok {
		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var c *Col
		// TODO: HERE
		xx, _ := s.Data().ElementString(0)
		fld := *xx
		if s.DataType() == d.DTstring {
			// TODO: check this may not work
			//			fld = s.Dialect().ToString(fld)
			fld = df.Dialect().ToString(fld)
		}

		c, _ = NewColSQL(s.DataType(), nil, fld, d.ColName(s.Name()))

		return c
	}

	panic("can't make column")
}

func getSQL(df d.DF, inputs ...d.Column) []string {
	var sOut []string
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, toCol(df, inputs[ind]).SQL())
	}

	return sOut
}

func getDataTypes(df d.DF, inputs ...d.Column) []d.DataTypes {
	var sOut []d.DataTypes
	for ind := 0; ind < len(inputs); ind++ {
		sOut = append(sOut, toCol(df, inputs[ind]).DataType())
	}

	return sOut
}

func fnGen(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes, info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: name, Inputs: inp, Output: outp}
	}

	sqls := getSQL(df, inputs...)
	dts := getDataTypes(df, inputs...)

	var sa []any
	for j := 0; j < len(sqls); j++ {
		sa = append(sa, sqls[j])
	}

	sqlOut := fmt.Sprintf(sql, sa...)

	outType := outp[0]
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

	outCol, _ := NewColSQL(outType, df.Dialect(), sqlOut)

	// TODO: think about best place to do this
	_ = d.ColParent(df)(outCol)
	_ = d.ColDialect(df.Dialect())(outCol)

	return &d.FnReturn{Value: outCol}
}
