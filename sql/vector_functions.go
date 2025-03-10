package sql

import (
	"fmt"

	d "github.com/invertedv/df"
)

func fnDefs(dlct *d.Dialect) d.Fns {
	var fns d.Fns
	for _, v := range dlct.Functions() {
		fns = append(fns,
			buildFn(v.Name, v.FnDetail, v.Inputs, v.Outputs, v.RT))
	}

	return fns
}

func buildFn(name, sql string, inp [][]d.DataTypes, outp []d.DataTypes, rt d.ReturnTypes) d.Fn {
	fn := func(info bool, df d.DF, inputs ...any) *d.FnReturn {
		if info {
			return &d.FnReturn{Name: name, Inputs: inp, Output: outp, RT: rt}
		}

		// glb flags if this is a global query, only meaningful if we have a GROUP BY
		glb := getGlobal(inputs...) && (df.(*DF).GroupBy() != "")
		sqls := getSQL(df, inputs...)
		dts := getDataTypes(df, inputs...)

		var sa []any
		for j := range len(sqls) {
			sa = append(sa, sqls[j])
		}

		sqlOut := fmt.Sprintf(sql, sa...)

		// if this returns a scalar, but there is no GROUP BY, then make a column of the global value
		// if you want to return the global value within a GROUP BY, put it within the function "global"
		if rt == d.RTscalar && df.(*DF).groupBy == "" {
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
			sqlOut, _ = df.Dialect().CastField(sqlOut, d.DTany, d.DTfloat)
		}

		outCol, _ := NewColSQL(outType, df.Dialect(), sqlOut)
		outCol.global = glb

		_ = d.ColParent(df)(outCol)
		_ = d.ColDialect(df.Dialect())(outCol)

		return &d.FnReturn{Value: outCol}
	}

	return fn
}

func global(info bool, df d.DF, inputs ...any) *d.FnReturn {
	if info {
		return &d.FnReturn{Name: "global", Inputs: [][]d.DataTypes{{d.DTany}}, Output: []d.DataTypes{d.DTany}, RT: d.RTcolumn}
	}

	sqls := getSQL(df, inputs...)
	dts := getDataTypes(df, inputs...)
	outCol, _ := NewColSQL(dts[0], df.Dialect(), sqls[0],  d.ColParent(df))
	// sends the signal back that this is a global query
	outCol.gf = true

	return &d.FnReturn{Value: outCol}
}


// ***************** categorical Operations *****************

func toCat(info bool, df d.DF, inputs ...any) *d.FnReturn {
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

func applyCat(info bool, df d.DF, inputs ...any) *d.FnReturn {
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

		c, _ = NewColSQL(s.DataType(), nil, fld, d.ColName(s.Name()),
			d.ColParent(df), d.ColDialect(df.Dialect()))

		return c
	}

	panic("can't make column")
}

func getSQL(df d.DF, inputs ...any) []string {
	var sOut []string
	for ind := range len(inputs) {
		col := toCol(df, inputs[ind])
		s, _ := col.SQL()
		sOut = append(sOut, s)
	}

	return sOut
}

// getDataTypes returns the d.DataTypes of the columns
func getDataTypes(df d.DF, inputs ...any) []d.DataTypes {
	var sOut []d.DataTypes
	for ind := range len(inputs) {
		sOut = append(sOut, toCol(df, inputs[ind]).DataType())
	}

	return sOut
}

// getGlobal returns true if any of the inputs has a gf signal (gf=used global() function)
func getGlobal(inputs ...any) bool {
	for ind := range len(inputs) {
		if col, ok := inputs[ind].(*Col); ok && col.gf {
			return col.gf
		}
	}

	return false
}
