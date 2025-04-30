package sql

import (
	"fmt"
	"strings"

	d "github.com/invertedv/df"
)

// fnDefs loads the d.Fns for use by Parse.
func fnDefs(dlct *d.Dialect) d.Fns {
	var fns d.Fns
	for _, v := range dlct.Functions() {
		if !v.Varying {
			fns = append(fns,
				buildFn(v.Name, v.FnDetail, v.Inputs, v.Outputs, v.IsScalar))
			continue
		}

		fns = append(fns,
			varying(v.Name, v.FnDetail, v.Inputs, v.Outputs))
	}

	return fns
}

// varying creates a d.Fn with a varying number of inputs from *.FnSpec. For the most part,
// this is used to create summary functions across columns (e.g. colSum, colMean).  It restricts the inputs to
// all having the same type.
func varying(fnName, sql string, inp [][]d.DataTypes, outp []d.DataTypes) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: fnName, Inputs: nil,
				Output:  nil,
				Varying: true}
		}

		var (
			cols []*Col
		)
		for ind := range len(inputs) {
			col := toCol(df, inputs[ind])
			cols = append(cols, col)

			if ind > 0 {
				if cols[0].DataType() != col.DataType() {
					return &d.FnReturn{Err: fmt.Errorf("all entries to %s function must be same type", fnName)}
				}
			}
		}

		var i []d.DataTypes
		for ind := range len(inp) {
			i = append(i, inp[ind][0])
		}

		sqls := getSQL(df, inputs...)

		ind := d.Position(cols[0].DataType(), i)
		if ind < 0 {
			return &d.FnReturn{Err: fmt.Errorf("incompatable type to function %s", fnName)}
		}

		outType := outp[ind]

		var sqlOut string
		switch sql {
		case "colSum":
			sqlOut = strings.Join(sqls, "+")
		case "colMean":
			sqlOut = fmt.Sprintf("(%s)/%d", strings.Join(sqls, "+"), len(sqls))
		case "colVar", "colStd":
			sqlMean := fmt.Sprintf("((%s)/%d)", strings.Join(sqls, "+"), len(sqls))
			for ind, c := range sqls {
				sqls[ind] = fmt.Sprintf("(%s-%s)*(%s-%s)", c, sqlMean, c, sqlMean)
			}

			sqlOut = fmt.Sprintf("(%s)/(%d-1)", strings.Join(sqls, "+"), len(sqls))
			if sql == "colStd" {
				sqlOut = fmt.Sprintf("sqrt(%s)", sqlOut)
			}
		default:
			sqlOut = fmt.Sprintf("%s(%s)", sql, strings.Join(sqls, ","))
		}

		// we may need to explicitly cast float fields as float
		if outType == d.DTfloat && df.Dialect().CastFloat() {
			sqlOut, _ = df.Dialect().CastField(sqlOut, d.DTfloat)
		}

		outCol, _ := NewCol(outType, df.Dialect(), sqlOut)

		_ = d.ColParent(df)(outCol)
		_ = d.ColDialect(df.Dialect())(outCol)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}

// buildFn creates a d.Fn from *.FnSpec.
func buildFn(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes, scalar bool) d.Fn {
	fn := func(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp, IsScalar: scalar}
		}

		// glb flags if this is a global query, only meaningful if we have a GROUP BY
		glb := getGlobal(inputs...) && (df.(*DF).GroupBy() != "")
		sqls := getSQL(df, inputs...)
		dts := getDataTypes(df, inputs...)

		var sa []any
		for j := range len(sqls) {
			sa = append(sa, sqls[j])
		}

		sqlOut := sql
		if strings.Contains(sql, "%s") {
			sqlOut = fmt.Sprintf(sqlOut, sa...)
		} else {
			sqlOut = sql
			for ind := range len(sa) {
				sqlOut = strings.ReplaceAll(sqlOut, fmt.Sprintf("#%d", ind), fmt.Sprintf("%s", sa[ind]))
			}
		}

		// if this returns a scalar, but there is no GROUP BY, then make a column of the global value
		// if you want to return the global value within a GROUP BY, put it within the function "global"
		if (scalar && df.(*DF).groupBy == "") || glb {
			sqlOut = df.Dialect().Global(df.(*DF).SourceSQL(), sqlOut)
		}

		outType := outp[0]
		// output type
		for ind := range len(inp) {
			ok := true
			for j := range len(dts) {
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

		// we may need to explicitly cast float fields as float
		if outType == d.DTfloat && df.Dialect().CastFloat() {
			sqlOut, _ = df.Dialect().CastField(sqlOut, d.DTfloat)
		}

		outCol, _ := NewCol(outType, df.Dialect(), sqlOut)

		_ = d.ColParent(df)(outCol)
		_ = d.ColDialect(df.Dialect())(outCol)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}

// handles "global" function in parser.  This is used to indicate that the argument is a global calculation.  For instance,
// "mx := mean(global(x))" will populate every row of mx with the mean of the x based on all rows of x.
func global(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "global", Inputs: [][]d.DataTypes{{d.DTunknown}}, Output: []d.DataTypes{d.DTunknown}, IsScalar: false}
	}

	sqls := getSQL(df, inputs...)
	dts := getDataTypes(df, inputs...)
	outCol, _ := NewCol(dts[0], df.Dialect(), sqls[0], d.ColParent(df))
	// sends the signal back that this is a global query
	outCol.gf = true

	return &d.FnReturn{Value: outCol}
}

// ***************** Categorical Operations *****************

// toCat creates a categorical column -- for use in Parse. This is not a full implementation of the
// Categorical method.
//
// Inputs are:
//  1. Column to operate on
//  2. fuzz value (optional)
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
		f := toAny(inputs[1])

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

// applyCat is for use in Parse.
// - vector to apply cats to
// - existing categorical column to use as the source.
// - default if a new level is encountered.
func applyCat(info bool, df d.DF, inputs ...d.Column) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "applyCat", Inputs: [][]d.DataTypes{{d.DTint, d.DTcategorical, d.DTint},
			{d.DTstring, d.DTcategorical, d.DTstring}, {d.DTdate, d.DTcategorical, d.DTdate}},
			Output: []d.DataTypes{d.DTcategorical, d.DTcategorical, d.DTcategorical}}
	}

	newData := inputs[0].(*Col)
	oldData := inputs[1].(*Col)
	newVal := toCol(df, inputs[2])

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
	if defaultValue, ok = d.ToDataType(toAny(inputs[2]), newVal.DataType()); !ok {
		return &d.FnReturn{Err: fmt.Errorf("cannot convert default value")}
	}

	if _, ok := oldData.CategoryMap()[defaultValue]; !ok {
		return &d.FnReturn{Err: fmt.Errorf("default value in applyCat not an existing category level")}
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

// ***************** Helpers *****************

// toAny expects either a *Col or *d.Scalar input
// If *Col, it returns the first element of its data
// If *d.Scalar, it returns its value
func toAny(x any) any {
	if s, ok := x.(*d.Scalar); ok {
		return s.Data().Element(0)
	}

	if s, ok := x.(*Col); ok {
		return s.Data().Element(0)
	}

	panic(fmt.Errorf("can't make value"))
}

// toCol makes a *Col out of x -- this should always be possible
func toCol(df d.DF, x any) *Col {
	if c, ok := x.(*Col); ok {
		if c.Parent() == nil {
			_ = d.ColParent(df)(c)
		}

		if c.Dialect() == nil {
			_ = d.ColDialect(df.Dialect())(c)
		}

		return c
	}

	if s, ok := x.(*d.Scalar); ok {
		var c *Col
		xx, _ := s.Data().ElementString(0)
		fld := *xx
		if s.DataType() == d.DTstring {
			fld = df.Dialect().ToString(fld)
		}
		// we may need to explicitly cast float fields as float
		if s.DataType() == d.DTfloat && df.Dialect().CastFloat() {
			fld, _ = df.Dialect().CastField(fld, d.DTfloat)
		}

		c, _ = NewCol(s.DataType(), nil, fld, d.ColName(s.Name()),
			d.ColParent(df), d.ColDialect(df.Dialect()))

		return c
	}

	panic("can't make column")
}

func getSQL(df d.DF, inputs ...d.Column) []string {
	var sOut []string
	for ind := range len(inputs) {
		col := toCol(df, inputs[ind])
		s, _ := col.SQL()
		sOut = append(sOut, s)
	}

	return sOut
}

func getDataTypes(df d.DF, inputs ...d.Column) []d.DataTypes {
	var sOut []d.DataTypes
	for ind := range len(inputs) {
		sOut = append(sOut, toCol(df, inputs[ind]).DataType())
	}

	return sOut
}

// getGlobal returns true if any of the inputs has a gf signal (gf=used global() function)
func getGlobal(inputs ...d.Column) bool {
	for ind := range len(inputs) {
		if col, ok := inputs[ind].(*Col); ok && col.gf {
			return true
		}
	}

	return false
}
